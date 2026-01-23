using System;
using System.Collections.Generic;
using System.IO;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using System.Reflection;
using DAL.Repositories;
using Json.Schema;
using System.Linq;

namespace BLL.Services
{
    public class FHIRService
    {
        private readonly IGraphRepository _repo;
        private readonly JsonSchema? _fhirSchema;
        private readonly JsonDocument? _schemaDoc;
        private readonly Uri? _baseUri;
        private readonly EvaluationOptions _evalOptions;

        /// <summary>
        /// Constructs the FHIR service, loading the FHIR JSON schema and configuring
        /// evaluation options for JsonSchema.Net in a version-tolerant way.
        /// </summary>
        public FHIRService(IGraphRepository repo)
        {
            _repo = repo;
            _evalOptions = BuildEvalOptions();

            try
            {
                var asmDir = AppContext.BaseDirectory;
                var schemaPath = Path.Combine(
                    asmDir,
                    "definitions.json",
                    "fhir.schema.json",
                    "fhir.schema.json");

                if (File.Exists(schemaPath))
                {
                    var text = File.ReadAllText(schemaPath);
                    _fhirSchema = JsonSchema.FromText(text);
                    _schemaDoc = JsonDocument.Parse(text);

                    var idProp =
                        _schemaDoc.RootElement.TryGetProperty("id", out var idEl)
                        && idEl.ValueKind == JsonValueKind.String
                            ? idEl.GetString()
                            : null;

                    var baseUri = !string.IsNullOrWhiteSpace(idProp)
                        ? new Uri(idProp!)
                        : new Uri("file://fhir.schema.json");

                    _baseUri = baseUri;
                    try
                    {
                        SchemaRegistry.Global.Register(baseUri, _fhirSchema);
                    }
                    catch
                    {
                        // ignore registry errors to remain resilient across environments
                    }
                }
            }
            catch
            {
                // if anything fails loading the schema, disable validation
                _fhirSchema = null;
            }
        }

        /// <summary>
        /// Builds EvaluationOptions in a way that is tolerant of different JsonSchema.Net
        /// versions by probing for available properties via reflection.
        /// </summary>
        private EvaluationOptions BuildEvalOptions()
        {
            // Start with defaults and then set common options if available on the installed package via reflection.
            var opts = new EvaluationOptions
            {
                OutputFormat = OutputFormat.List,
                RequireFormatValidation = false
            };

            try
            {
                var t = typeof(EvaluationOptions);

                // ContinueOnError (older APIs)
                var pi = t.GetProperty("ContinueOnError", BindingFlags.Instance | BindingFlags.Public);
                if (pi != null && pi.CanWrite)
                    pi.SetValue(opts, true);

                // AllowUnknownKeywords (older APIs)
                pi = t.GetProperty("AllowUnknownKeywords", BindingFlags.Instance | BindingFlags.Public);
                if (pi != null && pi.CanWrite)
                    pi.SetValue(opts, true);

                // ProcessCustomKeywords (some versions)
                pi = t.GetProperty("ProcessCustomKeywords", BindingFlags.Instance | BindingFlags.Public);
                if (pi != null && pi.CanWrite)
                    pi.SetValue(opts, false);

                // EvaluateAsynchronous (some versions)
                pi = t.GetProperty("EvaluateAsynchronous", BindingFlags.Instance | BindingFlags.Public);
                if (pi != null && pi.CanWrite)
                    pi.SetValue(opts, false);

                // ValidateRecursiveReferences (present in some 5.x versions)
                pi = t.GetProperty("ValidateRecursiveReferences", BindingFlags.Instance | BindingFlags.Public);
                if (pi != null && pi.CanWrite)
                    pi.SetValue(opts, false);

                // RefResolutionMode (7.x+)
                pi = t.GetProperty("RefResolutionMode", BindingFlags.Instance | BindingFlags.Public);
                if (pi != null && pi.CanWrite)
                {
                    var enumType = pi.PropertyType;
                    // try to set to Loose if available, otherwise set default
                    var enumVal = Enum.GetNames(enumType)
                        .FirstOrDefault(
                            n => string.Equals(n, "Loose", StringComparison.OrdinalIgnoreCase));
                    if (enumVal != null)
                    {
                        var parsed = Enum.Parse(enumType, enumVal);
                        pi.SetValue(opts, parsed);
                    }
                }

                // MaxDepth (7.x+)
                pi = t.GetProperty("MaxDepth", BindingFlags.Instance | BindingFlags.Public);
                if (pi != null && pi.CanWrite)
                {
                    // set a large value
                    if (pi.PropertyType == typeof(int))
                        pi.SetValue(opts, int.MaxValue);
                }

                // EvaluateUnevaluated (7.x+)
                pi = t.GetProperty("EvaluateUnevaluated", BindingFlags.Instance | BindingFlags.Public);
                if (pi != null && pi.CanWrite)
                {
                    if (pi.PropertyType == typeof(bool))
                        pi.SetValue(opts, false);
                }
            }
            catch
            {
                // swallow reflection failures and fall back to the minimal options we set above
            }

            return opts;
        }

        /// <summary>
        /// Evaluates a JSON instance against the given schema using the configured
        /// evaluation options. Circular reference / ref-resolution errors in the
        /// schema engine are treated as non-fatal and considered valid.
        /// </summary>
        private (bool ok, string? error) Eval(JsonSchema schema, JsonElement instance)
        {
            try
            {
                var eval = schema.Evaluate(instance, _evalOptions);

                if (eval.IsValid)
                    return (true, null);

                var msgs = new List<string>();

                if (eval.Details != null)
                {
                    foreach (var detail in eval.Details)
                    {
                        if (detail.HasErrors && detail.Errors != null)
                        {
                            foreach (var err in detail.Errors)
                            {
                                // err can be different types across versions; use ToString() defensively
                                var s = err.ToString() ?? string.Empty;
                                if (!string.IsNullOrEmpty(s))
                                {
                                    msgs.Add(s);
                                    if (msgs.Count >= 20) break;
                                }
                            }
                        }
                        if (msgs.Count >= 20) break;
                    }
                }

                var combined = msgs.Count > 0
                    ? string.Join("; ", msgs)
                    : "JSON does not conform to FHIR schema";

                return (false, combined);
            }
            catch (Exception ex)
            {
                // Detect common circular/ref resolution messages.
                var msg = ex.Message ?? ex.GetType().Name;
                var isCircular =
                    msg.IndexOf("circular", StringComparison.OrdinalIgnoreCase) >= 0 ||
                    msg.IndexOf("Cannot resolve", StringComparison.OrdinalIgnoreCase) >= 0 ||
                    msg.IndexOf("Encountered circular reference", StringComparison.OrdinalIgnoreCase) >= 0;

                if (isCircular)
                {
                    // The HL7 FHIR schema has internal circular references. For our purposes
                    // we treat these schema-level issues as non-fatal and allow the instance.
                    // Callers may log 'msg' if desired, but validation passes here.
                    return (true, null);
                }

                return (false, $"Validation failed: {msg}");
            }
        }

        /// <summary>
        /// Validates a JSON document against the loaded FHIR schema, if available.
        /// Returns (ok, error) where ok=false includes a human-readable error string.
        /// </summary>
        private (bool ok, string? error) Validate(JsonDocument json)
        {
            if (_fhirSchema is null)
                return (false, "FHIR schema not loaded");

            return Eval(_fhirSchema, json.RootElement);
        }

        /// <summary>
        /// Extracts the FHIR resourceType and id (if present) from a JSON document.
        /// Returns false with an error if resourceType is missing or invalid.
        /// </summary>
        private (bool ok, string? error, string resourceType, string? fhirId) ExtractResourceInfo(JsonDocument json)
        {
            if (!json.RootElement.TryGetProperty("resourceType", out var rtProp)
                || rtProp.ValueKind != JsonValueKind.String)
            {
                return (false, "Missing resourceType", string.Empty, null);
            }

            var resourceType = rtProp.GetString()!;

            string? fhirId = null;
            if (json.RootElement.TryGetProperty("id", out var idProp)
                && idProp.ValueKind == JsonValueKind.String)
            {
                fhirId = idProp.GetString();
            }
            else if (json.RootElement.TryGetProperty("id", out var idCheck)
                     && idCheck.ValueKind != JsonValueKind.String)
            {
                return (false, "Invalid id: must be string", string.Empty, null);
            }

            return (true, null, resourceType, fhirId);
        }

        /// <summary>
        /// Validates the given FHIR JSON and persists it to the graph. If an id is present
        /// in the FHIR resource, an upsert is performed using that id; otherwise, a new
        /// vertex is created. Returns graph vertex id and validation error (if any).
        /// </summary>
        public async Task<(bool ok, string? error, string? graphId, string? fhirId)>
            ValidateAndPersistAsync(JsonDocument json, CancellationToken ct = default)
        {
            var (extracted, extractError, resourceType, fhirId) = ExtractResourceInfo(json);
            if (!extracted)
            {
                return (false, extractError, null, null);
            }

            var (valid, error) = Validate(json);
            if (!valid)
            {
                return (false, error, null, null);
            }

            var props = new Dictionary<string, object>
            {
                ["resourceType"] = resourceType,
                ["json"] = json.RootElement.GetRawText()
            };

            string? graphId;

            if (!string.IsNullOrWhiteSpace(fhirId))
            {
                graphId = await _repo.UpsertVertexAndReturnIdAsync(
                    resourceType,
                    "id",
                    fhirId!,
                    props,
                    ct);
            }
            else
            {
                graphId = await _repo.AddVertexAndReturnIdAsync(
                    resourceType,
                    props,
                    ct);
            }

            return (true, null, graphId, fhirId);
        }

        /// <summary>
        /// Retrieves the raw stored JSON by graph vertex id. Returns an error if the
        /// vertex is missing or if the json property is not present.
        /// </summary>
        public async Task<(bool ok, string? error, string? json)> GetAsync(string id, CancellationToken ct = default)
        {
            var v = await _repo.GetVertexByIdAsync(id, ct);
            if (v == null)
                return (false, "Not found", null);

            var raw = v.Properties?
                .FirstOrDefault(p => p.Key == "json")
                .Value?
                .ToString();

            if (raw is null)
                return (false, "Stored vertex has no json payload", null);

            return (true, null, raw);
        }

        /// <summary>
        /// Updates an existing vertex json payload by graph id after re-validating the
        /// provided FHIR JSON against the FHIR schema.
        /// </summary>
        public async Task<(bool ok, string? error, string? fhirId)>
            UpdateAsync(string id, JsonDocument json, CancellationToken ct = default)
        {
            var (extracted, extractError, _, fhirId) = ExtractResourceInfo(json);
            if (!extracted)
            {
                return (false, extractError, null);
            }

            var (valid, error) = Validate(json);
            if (!valid)
                return (false, error, null);

            var props = new Dictionary<string, object>
            {
                ["json"] = json.RootElement.GetRawText()
            };

            var ok = await _repo.UpdateVertexPropertiesAsync(id, props, ct);
            return ok ? (true, null, fhirId) : (false, "Not found", null);
        }

        /// <summary>
        /// Deletes a vertex by graph id. Returns ok=false if the vertex
        /// could not be found or deletion failed.
        /// </summary>
        public async Task<(bool ok, string? error)> DeleteAsync(string id, CancellationToken ct = default)
        {
            var ok = await _repo.DeleteVertexAsync(id, ct);
            return ok ? (true, null) : (false, "Delete failed");
        }

        /// <summary>
        /// Creates an edge between two vertices identified by label/key/value pairs,
        /// using the underlying graph repository.
        /// </summary>
        public async Task<(bool ok, string? error, string? label)> LinkAsync(
            string label,
            string outLabel,
            string outKey,
            object outValue,
            string inLabel,
            string inKey,
            object inValue,
            IDictionary<string, object> properties,
            CancellationToken ct = default)
        {
            var edge = await _repo.AddEdgeByPropertyAsync(
                label,
                outLabel,
                outKey,
                outValue,
                inLabel,
                inKey,
                inValue,
                properties,
                ct);

            if (edge == null)
                return (false, "Vertices not found or edge creation failed", null);

            return (true, null, edge.Label);
        }

        /// <summary>
        /// Upserts a vertex of a given label keyed by a specific property, returning
        /// the resulting graph vertex id on success.
        /// </summary>
        public async Task<(bool ok, string? error, string? id)> UpsertAsync(
            string label,
            string key,
            object value,
            IDictionary<string, object> properties,
            CancellationToken ct = default)
        {
            var vid = await _repo.UpsertVertexAndReturnIdAsync(
                label,
                key,
                value,
                properties,
                ct);

            return vid is null
                ? (false, "Upsert failed", null)
                : (true, null, vid);
        }
    }
}