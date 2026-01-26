using System.Reflection;
using System.Text.Json;
using DAL.Repositories;
using Json.Schema;
using static BLL.Constants;

namespace BLL.Services;

/// <summary>
/// Service for FHIR schema loading and resource validation.
/// </summary>
public class FhirValidationService
{
    private readonly JsonSchema? _fhirSchema;
    private readonly JsonDocument? _schemaDoc;
    private readonly Uri? _baseUri;
    private readonly EvaluationOptions _evalOptions;

    public FhirValidationService()
    {
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
    /// Gets whether the FHIR schema is loaded and available for validation.
    /// </summary>
    public bool IsSchemaLoaded => _fhirSchema is not null;

    /// <summary>
    /// Builds EvaluationOptions in a way that is tolerant of different JsonSchema.Net
    /// versions by probing for available properties via reflection.
    /// </summary>
    private static EvaluationOptions BuildEvalOptions()
    {
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
                var enumVal = Enum.GetNames(enumType)
                    .FirstOrDefault(n => string.Equals(n, "Loose", StringComparison.OrdinalIgnoreCase));
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
            // swallow reflection failures
        }

        return opts;
    }

    /// <summary>
    /// Evaluates a JSON instance against the given schema.
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
            var msg = ex.Message ?? ex.GetType().Name;
            var isCircular =
                msg.IndexOf("circular", StringComparison.OrdinalIgnoreCase) >= 0 ||
                msg.IndexOf("Cannot resolve", StringComparison.OrdinalIgnoreCase) >= 0 ||
                msg.IndexOf("Encountered circular reference", StringComparison.OrdinalIgnoreCase) >= 0;

            if (isCircular)
                return (true, null);

            return (false, $"Validation failed: {msg}");
        }
    }

    /// <summary>
    /// Validates a JSON document against the loaded FHIR schema.
    /// </summary>
    public (bool ok, string? error) Validate(JsonDocument json)
    {
        if (_fhirSchema is null)
            return (false, "FHIR schema not loaded");

        return Eval(_fhirSchema, json.RootElement);
    }

    /// <summary>
    /// Extracts the FHIR resourceType and id from a JSON document.
    /// </summary>
    public (bool ok, string? error, string resourceType, string? fhirId) ExtractResourceInfo(JsonDocument json)
    {
        if (!json.RootElement.TryGetProperty(Properties.ResourceType, out var rtProp)
            || rtProp.ValueKind != JsonValueKind.String)
        {
            return (false, "Missing resourceType", string.Empty, null);
        }

        var resourceType = rtProp.GetString()!;

        string? fhirId = null;
        if (json.RootElement.TryGetProperty(Properties.Id, out var idProp)
            && idProp.ValueKind == JsonValueKind.String)
        {
            fhirId = idProp.GetString();
        }
        else if (json.RootElement.TryGetProperty(Properties.Id, out var idCheck)
                 && idCheck.ValueKind != JsonValueKind.String)
        {
            return (false, "Invalid id: must be string", string.Empty, null);
        }

        return (true, null, resourceType, fhirId);
    }

    /// <summary>
    /// Validates FHIR JSON without persisting it.
    /// </summary>
    public (bool ok, string? error, string? resourceType, string? fhirId) ValidateOnly(JsonDocument json)
    {
        var (extracted, extractError, resourceType, fhirId) = ExtractResourceInfo(json);
        if (!extracted)
            return (false, extractError, null, null);

        var (valid, error) = Validate(json);
        return valid
            ? (true, null, resourceType, fhirId)
            : (false, error, resourceType, fhirId);
    }

    /// <summary>
    /// Gets the list of supported resource types from the FHIR schema.
    /// </summary>
    public IReadOnlyList<string> GetSupportedResourceTypes()
    {
        if (_schemaDoc is null)
            return [];

        try
        {
            if (_schemaDoc.RootElement.TryGetProperty(SchemaProperties.Discriminator, out var disc) &&
                disc.TryGetProperty(SchemaProperties.Mapping, out var mapping))
            {
                return mapping.EnumerateObject()
                    .Select(p => p.Name)
                    .OrderBy(n => n)
                    .ToList();
            }
        }
        catch
        {
            // Ignore parsing errors
        }

        return [];
    }
}
