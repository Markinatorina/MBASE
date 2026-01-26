using Gremlin.Net.Driver;
using Gremlin.Net.Driver.Messages;
using Gremlin.Net.Process;
using Gremlin.Net.Structure;
using System.Text;
using System.Collections;

namespace DAL.Repositories;

public sealed record GraphVertex(string Id, string Label, IDictionary<string, object> Properties);

public sealed record GraphEdge(string Id, string Label, string OutVertexId, string InVertexId, IDictionary<string, object> Properties);

public sealed record GraphEdgeInspection(string Direction, string Label, string TargetVertexId, IDictionary<string, object> Properties);

public interface IGraphRepository
{
    Task<GraphVertex> AddVertexAsync(string label, IDictionary<string, object> properties, CancellationToken ct = default);
    Task<GraphEdge> AddEdgeAsync(string label, string outVertexId, string inVertexId, IDictionary<string, object>? properties = null, CancellationToken ct = default);
    Task<GraphVertex?> GetVertexByIdAsync(string id, CancellationToken ct = default);
    Task<bool> UpdateVertexPropertiesAsync(string id, IDictionary<string, object> properties, CancellationToken ct = default);
    Task<bool> DeleteVertexAsync(string id, CancellationToken ct = default);
    Task<long> CountVerticesAsync(CancellationToken ct = default);
    Task<GraphEdge?> AddEdgeByPropertyAsync(
        string label,
        string outLabel, string outKey, object outValue,
        string inLabel, string inKey, object inValue,
        IDictionary<string, object>? properties = null,
        CancellationToken ct = default);
    Task<GraphVertex> UpsertVertexByPropertyAsync(string label, string key, object value, IDictionary<string, object> properties, CancellationToken ct = default);
    Task<string?> AddVertexAndReturnIdAsync(string label, IDictionary<string, object> properties, CancellationToken ct = default);
    Task<string?> UpsertVertexAndReturnIdAsync(string label, string key, object value, IDictionary<string, object> properties, CancellationToken ct = default);

    Task<bool> EdgeExistsAsync(
        string label,
        string outVertexId,
        string inVertexId,
        CancellationToken ct = default);

    Task<string?> GetVertexIdByLabelAndPropertyAsync(
        string label,
        string key,
        object value,
        CancellationToken ct = default);

    Task<IReadOnlyCollection<GraphEdgeInspection>> GetEdgesForVertexAsync(string vertexId, CancellationToken ct = default);

    /// <summary>
    /// Gets vertices by label with optional property filters. Returns up to <paramref name="limit"/> results.
    /// </summary>
    Task<IReadOnlyCollection<GraphVertex>> GetVerticesByLabelAsync(
        string label,
        IDictionary<string, object>? filters = null,
        int limit = 100,
        int offset = 0,
        CancellationToken ct = default);

    /// <summary>
    /// Gets a full vertex (with all properties) by label and a specific property value.
    /// </summary>
    Task<GraphVertex?> GetVertexByLabelAndPropertyAsync(
        string label,
        string key,
        object value,
        CancellationToken ct = default);

    /// <summary>
    /// Counts vertices matching a label and optional property filters.
    /// </summary>
    Task<long> CountVerticesByLabelAsync(
        string label,
        IDictionary<string, object>? filters = null,
        CancellationToken ct = default);

    /// <summary>
    /// Gets vertices connected via outgoing edges from the source vertex.
    /// </summary>
    Task<IReadOnlyCollection<GraphVertex>> GetOutNeighborsAsync(
        string vertexId,
        string? edgeLabel = null,
        int limit = 100,
        CancellationToken ct = default);

    /// <summary>
    /// Gets vertices connected via incoming edges to the source vertex.
    /// </summary>
    Task<IReadOnlyCollection<GraphVertex>> GetInNeighborsAsync(
        string vertexId,
        string? edgeLabel = null,
        int limit = 100,
        CancellationToken ct = default);

    /// <summary>
    /// Performs an N-hop traversal from a source vertex, returning all reachable vertices.
    /// </summary>
    Task<IReadOnlyCollection<GraphVertex>> TraverseAsync(
        string vertexId,
        int maxHops = 2,
        string? edgeLabelFilter = null,
        int limit = 100,
        CancellationToken ct = default);

    /// <summary>
    /// Drops all vertices and edges from the graph. Use with caution.
    /// </summary>
    Task<long> DropAllAsync(CancellationToken ct = default);

    #region Versioning Support

    /// <summary>
    /// Gets the current (latest) version of a resource by label and FHIR id.
    /// Only returns vertices where isCurrent=true and isDeleted is not true.
    /// </summary>
    Task<GraphVertex?> GetCurrentVersionAsync(
        string label,
        string fhirId,
        CancellationToken ct = default);

    /// <summary>
    /// Gets a specific version of a resource (vread operation).
    /// </summary>
    Task<GraphVertex?> GetVersionAsync(
        string label,
        string fhirId,
        string versionId,
        CancellationToken ct = default);

    /// <summary>
    /// Gets the version history of a resource, ordered by lastUpdated descending.
    /// </summary>
    Task<IReadOnlyCollection<GraphVertex>> GetVersionHistoryAsync(
        string label,
        string fhirId,
        int limit = 100,
        CancellationToken ct = default);

    /// <summary>
    /// Gets the version history of all resources of a type, ordered by lastUpdated descending.
    /// </summary>
    Task<IReadOnlyCollection<GraphVertex>> GetTypeHistoryAsync(
        string label,
        int limit = 100,
        DateTime? since = null,
        CancellationToken ct = default);

    /// <summary>
    /// Creates a new versioned vertex. If a current version exists, it is marked as non-current
    /// and a supersedes edge is created from the new version to the old version.
    /// </summary>
    Task<(string graphId, string versionId)> CreateVersionedVertexAsync(
        string label,
        string fhirId,
        IDictionary<string, object> properties,
        CancellationToken ct = default);

    /// <summary>
    /// Creates a tombstone version for a soft delete. The resource is marked as deleted
    /// but the version history is preserved.
    /// </summary>
    Task<(string graphId, string versionId)?> CreateTombstoneAsync(
        string label,
        string fhirId,
        CancellationToken ct = default);

    /// <summary>
    /// Permanently deletes all versions of a resource (delete-history operation).
    /// </summary>
    Task<int> DeleteAllVersionsAsync(
        string label,
        string fhirId,
        CancellationToken ct = default);

    /// <summary>
    /// Permanently deletes a specific version of a resource.
    /// </summary>
    Task<bool> DeleteVersionAsync(
        string label,
        string fhirId,
        string versionId,
        CancellationToken ct = default);

    /// <summary>
    /// Gets the next version number for a resource.
    /// </summary>
    Task<int> GetNextVersionNumberAsync(
        string label,
        string fhirId,
        CancellationToken ct = default);

    #endregion
}

internal sealed class GraphRepository : IGraphRepository
{
    private readonly GremlinClient _client;

    /// <summary>
    /// Operation names for diagnostics and logging.
    /// </summary>
    private static class Ops
    {
        public const string AddVertex = nameof(AddVertex);
        public const string AddVertexAndReturnId = nameof(AddVertexAndReturnId);
        public const string AddEdge = nameof(AddEdge);
        public const string GetVertexById = nameof(GetVertexById);
        public const string UpdateVertexProperties = nameof(UpdateVertexProperties);
        public const string DeleteVertex = nameof(DeleteVertex);
        public const string CountVertices = nameof(CountVertices);
        public const string AddEdgeByProperty = nameof(AddEdgeByProperty);
        public const string UpsertVertexByProperty = nameof(UpsertVertexByProperty);
        public const string UpsertVertexAndReturnId = nameof(UpsertVertexAndReturnId);
        public const string EdgeExists = nameof(EdgeExists);
        public const string GetVertexIdByLabelAndProperty = nameof(GetVertexIdByLabelAndProperty);
        public const string GetVertexValueMapById = nameof(GetVertexValueMapById);
        public const string GetEdgesForVertex = nameof(GetEdgesForVertex);
        public const string DropAll = nameof(DropAll);
        public const string GetVerticesByLabel = nameof(GetVerticesByLabel);
        public const string GetVertexByLabelAndProperty = nameof(GetVertexByLabelAndProperty);
        public const string CountVerticesByLabel = nameof(CountVerticesByLabel);
        public const string GetOutNeighbors = nameof(GetOutNeighbors);
        public const string GetInNeighbors = nameof(GetInNeighbors);
        public const string Traverse = nameof(Traverse);

        // Versioning operations
        public const string GetCurrentVersion = nameof(GetCurrentVersion);
        public const string GetVersion = nameof(GetVersion);
        public const string GetVersionHistory = nameof(GetVersionHistory);
        public const string GetTypeHistory = nameof(GetTypeHistory);
        public const string GetMaxVersionNumber = nameof(GetMaxVersionNumber);
        public const string MarkVersionNonCurrent = nameof(MarkVersionNonCurrent);
        public const string CreateSupersedesEdge = nameof(CreateSupersedesEdge);
        public const string DeleteAllVersions = nameof(DeleteAllVersions);
        public const string DeleteVersion = nameof(DeleteVersion);
    }

    /// <summary>
    /// Gremlin script templates and constants.
    /// </summary>
    private static class Scripts
    {
        // Basic vertex operations
        public const string AddVertex = "g.addV(vertexLbl)";
        public const string GetVertexById = "g.V(vertexId)";
        public const string GetVertexValueMapById = "g.V(vertexId).valueMap(true)";
        public const string DeleteVertex = "g.V(vertexId).drop()";
        public const string CountVertices = "g.V().count()";
        public const string DropAll = "g.V().drop()";

        // Upsert operations
        public const string UpsertVertexByProperty = "g.V().has(lbl, propKey, propVal).fold().coalesce(unfold(), addV(lbl).property(propKey, propVal))";

        // Edge operations
        public const string AddEdge = "g.V(outId).addE(edgeLbl).to(__.V(inId))";
        public const string AddEdgeByProperty = "g.V().has(outLbl, outKey, outVal).addE(edgeLbl).to(__.V().has(inLbl, inKey, inVal))";
        public const string EdgeExists = "g.V(outId).outE(edgeLbl).inV().hasId(inId).count()";

        // Lookup operations
        public const string GetVertexIdByLabelAndProperty = "g.V().hasLabel(lbl).has(propKey, propVal).limit(1).id()";
        public const string GetVertexByLabelAndProperty = "g.V().hasLabel(lbl).has(propKey, propVal).limit(1).valueMap(true)";

        // Base scripts for dynamic query building
        public const string VerticesByLabelBase = "g.V().hasLabel(lbl)";
        
        // Neighbor traversal templates (use string.Format with limit parameter)
        public const string OutNeighborsAll = "g.V(vertexId).out().limit({0}).valueMap(true)";
        public const string OutNeighborsByEdge = "g.V(vertexId).out(edgeLbl).limit({0}).valueMap(true)";
        public const string InNeighborsAll = "g.V(vertexId).in().limit({0}).valueMap(true)";
        public const string InNeighborsByEdge = "g.V(vertexId).in(edgeLbl).limit({0}).valueMap(true)";

        // N-hop traversal templates (use string.Format with traversalStep, maxHops, limit)
        public const string TraverseTemplate = "g.V(vertexId).repeat({0}.simplePath()).times({1}).emit().dedup().limit({2}).valueMap(true)";
        public const string TraverseStepAll = "both()";
        public const string TraverseStepByEdge = "both(edgeLbl)";

        // Returns a list of maps with: direction, label, targetVertexId, properties
        // Uses valueMap() for edge props to avoid JanusGraph-specific types.
        public const string GetEdgesForVertex = 
            "g.V(vertexId).as('v')" +
            ".union(" +
            "  outE().as('e').inV().as('t')" +
            "    .project('direction','label','targetVertexId','properties')" +
            "    .by(constant('out'))" +
            "    .by(select('e').label())" +
            "    .by(select('t').id())" +
            "    .by(select('e').valueMap())," +
            "  inE().as('e').outV().as('t')" +
            "    .project('direction','label','targetVertexId','properties')" +
            "    .by(constant('in'))" +
            "    .by(select('e').label())" +
            "    .by(select('t').id())" +
            "    .by(select('e').valueMap())" +
            ")";

        // Query suffixes
        public const string SuffixId = ".id()";
        public const string SuffixCount = ".count()";
        public const string SuffixValueMapTrue = ".valueMap(true)";

        #region Versioning Scripts

        // Version property names
        public const string PropVersionId = "versionId";
        public const string PropLastUpdated = "lastUpdated";
        public const string PropIsCurrent = "isCurrent";
        public const string PropIsDeleted = "isDeleted";
        public const string PropFhirId = "id";

        // Edge label for version chain
        public const string EdgeSupersedes = "supersedes";

        // Get current version: hasLabel, has(id), has(isCurrent, true), not deleted
        public const string GetCurrentVersion = 
            "g.V().hasLabel(lbl).has('id', fhirId).has('isCurrent', true).not(has('isDeleted', true)).limit(1).valueMap(true)";

        // Get specific version (vread)
        public const string GetVersion = 
            "g.V().hasLabel(lbl).has('id', fhirId).has('versionId', versionId).limit(1).valueMap(true)";

        // Get version history ordered by lastUpdated desc
        public const string GetVersionHistory = 
            "g.V().hasLabel(lbl).has('id', fhirId).order().by('lastUpdated', desc).limit({0}).valueMap(true)";

        // Get type history (all resources of a type) ordered by lastUpdated desc
        public const string GetTypeHistory = 
            "g.V().hasLabel(lbl).order().by('lastUpdated', desc).limit({0}).valueMap(true)";

        // Get type history since a date
        public const string GetTypeHistorySince = 
            "g.V().hasLabel(lbl).has('lastUpdated', gte(sinceDate)).order().by('lastUpdated', desc).limit({0}).valueMap(true)";

        // Get max version number for a resource
        public const string GetMaxVersionNumber = 
            "g.V().hasLabel(lbl).has('id', fhirId).values('versionId').max()";

        // Mark a version as non-current
        public const string MarkVersionNonCurrent = 
            "g.V().hasLabel(lbl).has('id', fhirId).has('isCurrent', true).property('isCurrent', false)";

        // Create supersedes edge from new version to old version
        public const string CreateSupersedesEdge = 
            "g.V(newVersionId).addE('supersedes').to(__.V(oldVersionId)).count()";

        // Delete all versions of a resource
        public const string DeleteAllVersions = 
            "g.V().hasLabel(lbl).has('id', fhirId).drop()";

        // Delete specific version
        public const string DeleteSingleVersion = 
            "g.V().hasLabel(lbl).has('id', fhirId).has('versionId', versionId).drop()";

        // Count versions for a resource
        public const string CountVersions = 
            "g.V().hasLabel(lbl).has('id', fhirId).count()";

        #endregion
        
        /// <summary>
        /// Builds a range suffix for pagination: .range(offset, offset+limit)
        /// </summary>
        public static string RangeSuffix(int offset, int limit) => $".range({offset}, {offset + limit})";
        
        /// <summary>
        /// Builds a dynamic filter clause: .has(keyBinding, valBinding)
        /// </summary>
        public static string HasClause(string keyBinding, string valBinding) => $".has({keyBinding}, {valBinding})";
    }

    public GraphRepository(GremlinClient client)
    {
        _client = client;
    }

    private static string FormatBindingsForDiagnostic(Dictionary<string, object>? bindings)
    {
        if (bindings is null || bindings.Count == 0)
            return "<none>";

        // Only include keys (never values) to avoid leaking payloads.
        return string.Join(", ", bindings.Keys.OrderBy(k => k, StringComparer.Ordinal));
    }

    private static void AppendProperties(StringBuilder script, Dictionary<string, object> bindings, IDictionary<string, object>? properties)
    {
        if (properties is null || properties.Count == 0)
            return;

        foreach (var (key, value) in properties)
        {
            var bindingKey = $"p_{key}";
            script.Append($".property('{key}', {bindingKey})");
            bindings[bindingKey] = value!;
        }
    }

    private async Task<IReadOnlyCollection<T>> SubmitAsync<T>(
        string operation,
        string script,
        Dictionary<string, object>? bindings,
        CancellationToken ct)
    {
        try
        {
            var messageBuilder = RequestMessage.Build(Tokens.OpsEval)
                .AddArgument(Tokens.ArgsGremlin, script);

            if (bindings is not null && bindings.Count > 0)
            {
                messageBuilder = messageBuilder.AddArgument(Tokens.ArgsBindings, bindings);
            }

            var message = messageBuilder.Create();
            return await _client.SubmitAsync<T>(message, ct);
        }
        catch (Exception ex)
        {
            var bindingsInfo = FormatBindingsForDiagnostic(bindings);

            throw new InvalidOperationException(
                $"Gremlin operation '{operation}' failed. Bindings(keys)=[{bindingsInfo}] Script={script}",
                ex);
        }
    }

    private static IDictionary<string, object> MaterializeProperties(object? input)
    {
        if (input is null)
            return new Dictionary<string, object>();

        if (input is IDictionary dict)
        {
            var result = new Dictionary<string, object>(StringComparer.Ordinal);
            foreach (DictionaryEntry entry in dict)
            {
                var key = entry.Key?.ToString();
                if (string.IsNullOrWhiteSpace(key))
                    continue;

                result[key] = UnwrapGremlinMapValue(entry.Value);
            }
            return result;
        }

        // Some Gremlin.Net shapes can come back as arrays/enumerables of properties.
        // Represent as a single "value" field rather than leaking Gremlin types.
        return new Dictionary<string, object>(StringComparer.Ordinal)
        {
            ["value"] = input
        };
    }

    private static object UnwrapGremlinMapValue(object? value)
    {
        if (value is null)
            return string.Empty;

        // Gremlin valueMap(true) commonly returns property values as lists.
        if (value is IList list)
        {
            if (list.Count == 1)
                return UnwrapGremlinMapValue(list[0]);

            // If this is a list of non-string objects, keep as a list but unwrap items.
            var unwrap = new List<object>(list.Count);
            foreach (var item in list)
            {
                unwrap.Add(UnwrapGremlinMapValue(item));
            }
            return unwrap;
        }

        return value;
    }

    private static GraphVertex ToGraphVertex(Vertex v)
    {
        var id = v.Id?.ToString() ?? string.Empty;
        var props = MaterializeProperties(v.Properties);
        return new GraphVertex(id, v.Label ?? string.Empty, props);
    }

    private static GraphEdge ToGraphEdge(Edge e)
    {
        var id = e.Id?.ToString() ?? string.Empty;
        var outV = e.OutV?.Id?.ToString() ?? string.Empty;
        var inV = e.InV?.Id?.ToString() ?? string.Empty;

        var props = MaterializeProperties(e.Properties);

        return new GraphEdge(id, e.Label ?? string.Empty, outV, inV, props);
    }

    public async Task<GraphVertex> AddVertexAsync(string label, IDictionary<string, object> properties, CancellationToken ct = default)
    {
        var script = new StringBuilder(Scripts.AddVertex);
        var bindings = new Dictionary<string, object> { ["vertexLbl"] = label };

        AppendProperties(script, bindings, properties);

        var result = await SubmitAsync<Vertex>(Ops.AddVertex, script.ToString(), bindings, ct);
        return ToGraphVertex(result.First());
    }

    public async Task<string?> AddVertexAndReturnIdAsync(string label, IDictionary<string, object> properties, CancellationToken ct = default)
    {
        var script = new StringBuilder(Scripts.AddVertex);
        var bindings = new Dictionary<string, object> { ["vertexLbl"] = label };

        AppendProperties(script, bindings, properties);

        script.Append(Scripts.SuffixId);
        var result = await SubmitAsync<object>(Ops.AddVertexAndReturnId, script.ToString(), bindings, ct);
        return result.FirstOrDefault()?.ToString();
    }

    public async Task<GraphEdge> AddEdgeAsync(string label, string outVertexId, string inVertexId, IDictionary<string, object>? properties = null, CancellationToken ct = default)
    {
        var script = new StringBuilder(Scripts.AddEdge);
        var bindings = new Dictionary<string, object>
        {
            ["edgeLbl"] = label,
            ["outId"] = outVertexId,
            ["inId"] = inVertexId
        };

        AppendProperties(script, bindings, properties);

        // JanusGraph uses RelationIdentifier for edge IDs which Gremlin.Net can't deserialize.
        // Return a count instead to verify the edge was created; construct the GraphEdge from known parameters.
        script.Append(Scripts.SuffixCount);
        var result = await SubmitAsync<long>(Ops.AddEdge, script.ToString(), bindings, ct);
        
        if (result.FirstOrDefault() == 0)
            throw new InvalidOperationException($"Failed to create edge '{label}' from {outVertexId} to {inVertexId}");

        return new GraphEdge(
            string.Empty, // Edge ID not available due to JanusGraph serialization issues
            label,
            outVertexId,
            inVertexId,
            properties ?? new Dictionary<string, object>());
    }

    public async Task<GraphVertex?> GetVertexByIdAsync(string id, CancellationToken ct = default)
    {
        var bindings = new Dictionary<string, object> { ["vertexId"] = id };

        // JanusGraph can return types (e.g. RelationIdentifier) that Gremlin.Net doesn't
        // have GraphBinary serializers for when deserializing full Vertex/VertexProperty.
        // Use valueMap(true) to retrieve a plain map instead.
        var result = await SubmitAsync<IDictionary>(
            Ops.GetVertexValueMapById,
            Scripts.GetVertexValueMapById,
            bindings,
            ct);

        var map = result.FirstOrDefault();
        if (map is null)
            return null;

        var props = MaterializeProperties(map);

        var graphId = props.TryGetValue("id", out var idObj) ? idObj?.ToString() ?? string.Empty : string.Empty;
        var label = props.TryGetValue("label", out var lblObj) ? lblObj?.ToString() ?? string.Empty : string.Empty;

        // Ensure these are simple strings for downstream callers.
        props["id"] = graphId;
        props["label"] = label;

        return new GraphVertex(graphId, label, props);
    }

    public async Task<bool> UpdateVertexPropertiesAsync(string vertexId, IDictionary<string, object> properties, CancellationToken ct = default)
    {
        var script = new StringBuilder(Scripts.GetVertexById);
        var bindings = new Dictionary<string, object> { ["vertexId"] = vertexId };

        AppendProperties(script, bindings, properties);

        var result = await SubmitAsync<Vertex>(Ops.UpdateVertexProperties, script.ToString(), bindings, ct);
        return result.Any();
    }

    public async Task<bool> DeleteVertexAsync(string id, CancellationToken ct = default)
    {
        var result = await SubmitAsync<object>(Ops.DeleteVertex, Scripts.DeleteVertex, new Dictionary<string, object> { ["vertexId"] = id }, ct);
        // JanusGraph returns empty result set for drop; treat as success if no exception
        return true;
    }

    public async Task<long> CountVerticesAsync(CancellationToken ct = default)
    {
        var result = await SubmitAsync<long>(Ops.CountVertices, Scripts.CountVertices, null, ct);
        return result.FirstOrDefault();
    }

    public async Task<GraphEdge?> AddEdgeByPropertyAsync(
        string label,
        string outLabel, string outKey, object outValue,
        string inLabel, string inKey, object inValue,
        IDictionary<string, object>? properties = null,
        CancellationToken ct = default)
    {
        var script = new StringBuilder(Scripts.AddEdgeByProperty);
        var bindings = new Dictionary<string, object>
        {
            ["edgeLbl"] = label,
            ["outLbl"] = outLabel,
            ["outKey"] = outKey,
            ["outVal"] = outValue,
            ["inLbl"] = inLabel,
            ["inKey"] = inKey,
            ["inVal"] = inValue
        };

        AppendProperties(script, bindings, properties);

        // JanusGraph uses RelationIdentifier for edge IDs which Gremlin.Net can't deserialize.
        // Return count to verify edge was created.
        script.Append(Scripts.SuffixCount);
        var result = await SubmitAsync<long>(Ops.AddEdgeByProperty, script.ToString(), bindings, ct);

        if (result.FirstOrDefault() == 0)
            return null;

        return new GraphEdge(
            string.Empty, // Edge ID not available due to JanusGraph serialization issues
            label,
            string.Empty, // outVertexId not known without additional query
            string.Empty, // inVertexId not known without additional query
            properties ?? new Dictionary<string, object>());
    }

    public async Task<GraphVertex> UpsertVertexByPropertyAsync(string label, string key, object value, IDictionary<string, object> properties, CancellationToken ct = default)
    {
        var script = new StringBuilder(Scripts.UpsertVertexByProperty);
        var bindings = new Dictionary<string, object>
        {
            ["lbl"] = label,
            ["propKey"] = key,
            ["propVal"] = value
        };

        AppendProperties(script, bindings, properties);

        var result = await SubmitAsync<Vertex>(Ops.UpsertVertexByProperty, script.ToString(), bindings, ct);
        return ToGraphVertex(result.First());
    }

    public async Task<string?> UpsertVertexAndReturnIdAsync(string label, string key, object value, IDictionary<string, object> properties, CancellationToken ct = default)
    {
        var script = new StringBuilder(Scripts.UpsertVertexByProperty);
        var bindings = new Dictionary<string, object>
        {
            ["lbl"] = label,
            ["propKey"] = key,
            ["propVal"] = value
        };

        AppendProperties(script, bindings, properties);

        script.Append(Scripts.SuffixId);
        var result = await SubmitAsync<object>(Ops.UpsertVertexAndReturnId, script.ToString(), bindings, ct);
        return result.FirstOrDefault()?.ToString();
    }

    public async Task<bool> EdgeExistsAsync(
        string label,
        string outVertexId,
        string inVertexId,
        CancellationToken ct = default)
    {
        var bindings = new Dictionary<string, object>
        {
            ["edgeLbl"] = label,
            ["outId"] = outVertexId,
            ["inId"] = inVertexId
        };

        // Returns count of matching edges; avoids returning Edge objects that contain
        // JanusGraph's RelationIdentifier which Gremlin.Net can't deserialize.
        var result = await SubmitAsync<long>(Ops.EdgeExists, Scripts.EdgeExists, bindings, ct);
        return result.FirstOrDefault() > 0;
    }

    public async Task<string?> GetVertexIdByLabelAndPropertyAsync(
        string label,
        string key,
        object value,
        CancellationToken ct = default)
    {
        // Ensure we compare on string for FHIR identifiers and avoid type-mismatch lookups.
        // (FHIRService persists `id` as a string property.)
        if (value is not string)
            value = value.ToString() ?? string.Empty;

        var bindings = new Dictionary<string, object>
        {
            ["lbl"] = label,
            ["propKey"] = key,
            ["propVal"] = value
        };

        var result = await SubmitAsync<object>(
            Ops.GetVertexIdByLabelAndProperty,
            Scripts.GetVertexIdByLabelAndProperty,
            bindings,
            ct);

        return result.FirstOrDefault()?.ToString();
    }

    public async Task<IReadOnlyCollection<GraphEdgeInspection>> GetEdgesForVertexAsync(string vertexId, CancellationToken ct = default)
    {
        var bindings = new Dictionary<string, object> { ["vertexId"] = vertexId };

        var rows = await SubmitAsync<IDictionary>(
            Ops.GetEdgesForVertex,
            Scripts.GetEdgesForVertex,
            bindings,
            ct);

        var result = new List<GraphEdgeInspection>(rows.Count);
        foreach (var row in rows)
        {
            if (row is null)
                continue;

            var props = MaterializeProperties(row);

            var direction = props.TryGetValue("direction", out var d) ? d?.ToString() ?? string.Empty : string.Empty;
            var label = props.TryGetValue("label", out var l) ? l?.ToString() ?? string.Empty : string.Empty;
            var targetVertexId = props.TryGetValue("targetVertexId", out var t) ? t?.ToString() ?? string.Empty : string.Empty;

            IDictionary<string, object> edgeProps = new Dictionary<string, object>(StringComparer.Ordinal);
            if (props.TryGetValue("properties", out var p))
            {
                edgeProps = MaterializeProperties(p);
            }

            result.Add(new GraphEdgeInspection(direction, label, targetVertexId, edgeProps));
        }

        return result;
    }

    public async Task<long> DropAllAsync(CancellationToken ct = default)
    {
        var countBefore = await CountVerticesAsync(ct);
        await SubmitAsync<object>(Ops.DropAll, Scripts.DropAll, null, ct);
        return countBefore;
    }

    public async Task<IReadOnlyCollection<GraphVertex>> GetVerticesByLabelAsync(
        string label,
        IDictionary<string, object>? filters = null,
        int limit = 100,
        int offset = 0,
        CancellationToken ct = default)
    {
        var script = new StringBuilder(Scripts.VerticesByLabelBase);
        var bindings = new Dictionary<string, object> { ["lbl"] = label };

        // Add property filters dynamically
        if (filters is not null)
        {
            var filterIndex = 0;
            foreach (var (key, value) in filters)
            {
                var keyBinding = $"fk{filterIndex}";
                var valBinding = $"fv{filterIndex}";
                script.Append(Scripts.HasClause(keyBinding, valBinding));
                bindings[keyBinding] = key;
                bindings[valBinding] = value?.ToString() ?? string.Empty;
                filterIndex++;
            }
        }

        script.Append(Scripts.RangeSuffix(offset, limit));
        script.Append(Scripts.SuffixValueMapTrue);

        var rows = await SubmitAsync<IDictionary>(
            Ops.GetVerticesByLabel,
            script.ToString(),
            bindings,
            ct);

        return rows.Select(MapToGraphVertex).Where(v => v is not null).Cast<GraphVertex>().ToList();
    }

    public async Task<GraphVertex?> GetVertexByLabelAndPropertyAsync(
        string label,
        string key,
        object value,
        CancellationToken ct = default)
    {
        if (value is not string)
            value = value.ToString() ?? string.Empty;

        var bindings = new Dictionary<string, object>
        {
            ["lbl"] = label,
            ["propKey"] = key,
            ["propVal"] = value
        };

        var result = await SubmitAsync<IDictionary>(
            Ops.GetVertexByLabelAndProperty,
            Scripts.GetVertexByLabelAndProperty,
            bindings,
            ct);

        var map = result.FirstOrDefault();
        return map is null ? null : MapToGraphVertex(map);
    }

    public async Task<long> CountVerticesByLabelAsync(
        string label,
        IDictionary<string, object>? filters = null,
        CancellationToken ct = default)
    {
        var script = new StringBuilder(Scripts.VerticesByLabelBase);
        var bindings = new Dictionary<string, object> { ["lbl"] = label };

        if (filters is not null)
        {
            var filterIndex = 0;
            foreach (var (key, value) in filters)
            {
                var keyBinding = $"fk{filterIndex}";
                var valBinding = $"fv{filterIndex}";
                script.Append(Scripts.HasClause(keyBinding, valBinding));
                bindings[keyBinding] = key;
                bindings[valBinding] = value?.ToString() ?? string.Empty;
                filterIndex++;
            }
        }

        script.Append(Scripts.SuffixCount);

        var result = await SubmitAsync<long>(
            Ops.CountVerticesByLabel,
            script.ToString(),
            bindings,
            ct);

        return result.FirstOrDefault();
    }

    public async Task<IReadOnlyCollection<GraphVertex>> GetOutNeighborsAsync(
        string vertexId,
        string? edgeLabel = null,
        int limit = 100,
        CancellationToken ct = default)
    {
        var bindings = new Dictionary<string, object> { ["vertexId"] = vertexId };
        string script;

        if (!string.IsNullOrWhiteSpace(edgeLabel))
        {
            bindings["edgeLbl"] = edgeLabel;
            script = string.Format(Scripts.OutNeighborsByEdge, limit);
        }
        else
        {
            script = string.Format(Scripts.OutNeighborsAll, limit);
        }

        var rows = await SubmitAsync<IDictionary>(
            Ops.GetOutNeighbors,
            script,
            bindings,
            ct);

        return rows.Select(MapToGraphVertex).Where(v => v is not null).Cast<GraphVertex>().ToList();
    }

    public async Task<IReadOnlyCollection<GraphVertex>> GetInNeighborsAsync(
        string vertexId,
        string? edgeLabel = null,
        int limit = 100,
        CancellationToken ct = default)
    {
        var bindings = new Dictionary<string, object> { ["vertexId"] = vertexId };
        string script;

        if (!string.IsNullOrWhiteSpace(edgeLabel))
        {
            bindings["edgeLbl"] = edgeLabel;
            script = string.Format(Scripts.InNeighborsByEdge, limit);
        }
        else
        {
            script = string.Format(Scripts.InNeighborsAll, limit);
        }

        var rows = await SubmitAsync<IDictionary>(
            Ops.GetInNeighbors,
            script,
            bindings,
            ct);

        return rows.Select(MapToGraphVertex).Where(v => v is not null).Cast<GraphVertex>().ToList();
    }

    public async Task<IReadOnlyCollection<GraphVertex>> TraverseAsync(
        string vertexId,
        int maxHops = 2,
        string? edgeLabelFilter = null,
        int limit = 100,
        CancellationToken ct = default)
    {
        var bindings = new Dictionary<string, object> { ["vertexId"] = vertexId };

        // Use repeat().times() for N-hop traversal, emit() to collect all intermediate vertices
        string traversalStep;
        if (!string.IsNullOrWhiteSpace(edgeLabelFilter))
        {
            bindings["edgeLbl"] = edgeLabelFilter;
            traversalStep = Scripts.TraverseStepByEdge;
        }
        else
        {
            traversalStep = Scripts.TraverseStepAll;
        }

        var script = string.Format(Scripts.TraverseTemplate, traversalStep, maxHops, limit);

        var rows = await SubmitAsync<IDictionary>(
            Ops.Traverse,
            script,
            bindings,
            ct);

        return rows.Select(MapToGraphVertex).Where(v => v is not null).Cast<GraphVertex>().ToList();
    }

    private static GraphVertex? MapToGraphVertex(IDictionary? map)
    {
        if (map is null)
            return null;

        var props = MaterializeProperties(map);

        var graphId = props.TryGetValue("id", out var idObj) ? idObj?.ToString() ?? string.Empty : string.Empty;
        var label = props.TryGetValue("label", out var lblObj) ? lblObj?.ToString() ?? string.Empty : string.Empty;

        // Handle T.id and T.label if present (JanusGraph valueMap(true) format)
        if (string.IsNullOrEmpty(graphId) && props.TryGetValue("T.id", out var tId))
            graphId = tId?.ToString() ?? string.Empty;
        if (string.IsNullOrEmpty(label) && props.TryGetValue("T.label", out var tLbl))
            label = tLbl?.ToString() ?? string.Empty;

        props["id"] = graphId;
        props["label"] = label;

        return new GraphVertex(graphId, label, props);
    }

    #region Versioning Implementation

    public async Task<GraphVertex?> GetCurrentVersionAsync(
        string label,
        string fhirId,
        CancellationToken ct = default)
    {
        var bindings = new Dictionary<string, object>
        {
            ["lbl"] = label,
            ["fhirId"] = fhirId
        };

        var result = await SubmitAsync<IDictionary>(
            Ops.GetCurrentVersion,
            Scripts.GetCurrentVersion,
            bindings,
            ct);

        var map = result.FirstOrDefault();
        return map is null ? null : MapToGraphVertex(map);
    }

    public async Task<GraphVertex?> GetVersionAsync(
        string label,
        string fhirId,
        string versionId,
        CancellationToken ct = default)
    {
        var bindings = new Dictionary<string, object>
        {
            ["lbl"] = label,
            ["fhirId"] = fhirId,
            ["versionId"] = versionId
        };

        var result = await SubmitAsync<IDictionary>(
            Ops.GetVersion,
            Scripts.GetVersion,
            bindings,
            ct);

        var map = result.FirstOrDefault();
        return map is null ? null : MapToGraphVertex(map);
    }

    public async Task<IReadOnlyCollection<GraphVertex>> GetVersionHistoryAsync(
        string label,
        string fhirId,
        int limit = 100,
        CancellationToken ct = default)
    {
        var bindings = new Dictionary<string, object>
        {
            ["lbl"] = label,
            ["fhirId"] = fhirId
        };

        var script = string.Format(Scripts.GetVersionHistory, limit);

        var rows = await SubmitAsync<IDictionary>(
            Ops.GetVersionHistory,
            script,
            bindings,
            ct);

        return rows.Select(MapToGraphVertex).Where(v => v is not null).Cast<GraphVertex>().ToList();
    }

    public async Task<IReadOnlyCollection<GraphVertex>> GetTypeHistoryAsync(
        string label,
        int limit = 100,
        DateTime? since = null,
        CancellationToken ct = default)
    {
        var bindings = new Dictionary<string, object> { ["lbl"] = label };
        string script;

        if (since.HasValue)
        {
            bindings["sinceDate"] = since.Value.ToString("o");
            script = string.Format(Scripts.GetTypeHistorySince, limit);
        }
        else
        {
            script = string.Format(Scripts.GetTypeHistory, limit);
        }

        var rows = await SubmitAsync<IDictionary>(
            Ops.GetTypeHistory,
            script,
            bindings,
            ct);

        return rows.Select(MapToGraphVertex).Where(v => v is not null).Cast<GraphVertex>().ToList();
    }

    public async Task<int> GetNextVersionNumberAsync(
        string label,
        string fhirId,
        CancellationToken ct = default)
    {
        var bindings = new Dictionary<string, object>
        {
            ["lbl"] = label,
            ["fhirId"] = fhirId
        };

        var result = await SubmitAsync<object>(
            Ops.GetMaxVersionNumber,
            Scripts.GetMaxVersionNumber,
            bindings,
            ct);

        var maxVersion = result.FirstOrDefault();
        if (maxVersion is null)
            return 1;

        // Parse the version and increment
        if (int.TryParse(maxVersion.ToString(), out var version))
            return version + 1;

        return 1;
    }

    public async Task<(string graphId, string versionId)> CreateVersionedVertexAsync(
        string label,
        string fhirId,
        IDictionary<string, object> properties,
        CancellationToken ct = default)
    {
        // Get the next version number
        var nextVersion = await GetNextVersionNumberAsync(label, fhirId, ct);
        var versionId = nextVersion.ToString();
        var lastUpdated = DateTime.UtcNow.ToString("o");

        // Find current version to mark as non-current and link
        string? previousVersionGraphId = null;
        var currentVersion = await GetCurrentVersionAsync(label, fhirId, ct);
        if (currentVersion is not null)
        {
            previousVersionGraphId = currentVersion.Id;

            // Mark the current version as non-current
            var markBindings = new Dictionary<string, object>
            {
                ["lbl"] = label,
                ["fhirId"] = fhirId
            };
            await SubmitAsync<object>(Ops.MarkVersionNonCurrent, Scripts.MarkVersionNonCurrent, markBindings, ct);
        }

        // Add version properties to the new vertex
        var versionedProperties = new Dictionary<string, object>(properties)
        {
            [Scripts.PropFhirId] = fhirId,
            [Scripts.PropVersionId] = versionId,
            [Scripts.PropLastUpdated] = lastUpdated,
            [Scripts.PropIsCurrent] = true
        };

        // Create the new vertex
        var newGraphId = await AddVertexAndReturnIdAsync(label, versionedProperties, ct);
        if (newGraphId is null)
            throw new InvalidOperationException($"Failed to create versioned vertex for {label}/{fhirId}");

        // Create supersedes edge if there was a previous version
        if (previousVersionGraphId is not null)
        {
            var edgeBindings = new Dictionary<string, object>
            {
                ["newVersionId"] = newGraphId,
                ["oldVersionId"] = previousVersionGraphId
            };
            await SubmitAsync<long>(Ops.CreateSupersedesEdge, Scripts.CreateSupersedesEdge, edgeBindings, ct);
        }

        return (newGraphId, versionId);
    }

    public async Task<(string graphId, string versionId)?> CreateTombstoneAsync(
        string label,
        string fhirId,
        CancellationToken ct = default)
    {
        // Check if the resource exists and is not already deleted
        var currentVersion = await GetCurrentVersionAsync(label, fhirId, ct);
        if (currentVersion is null)
            return null;

        // Get next version number
        var nextVersion = await GetNextVersionNumberAsync(label, fhirId, ct);
        var versionId = nextVersion.ToString();
        var lastUpdated = DateTime.UtcNow.ToString("o");

        var previousVersionGraphId = currentVersion.Id;

        // Mark current version as non-current
        var markBindings = new Dictionary<string, object>
        {
            ["lbl"] = label,
            ["fhirId"] = fhirId
        };
        await SubmitAsync<object>(Ops.MarkVersionNonCurrent, Scripts.MarkVersionNonCurrent, markBindings, ct);

        // Create tombstone vertex (minimal properties, marked as deleted)
        var tombstoneProperties = new Dictionary<string, object>
        {
            [Scripts.PropFhirId] = fhirId,
            [Scripts.PropVersionId] = versionId,
            [Scripts.PropLastUpdated] = lastUpdated,
            [Scripts.PropIsCurrent] = true,
            [Scripts.PropIsDeleted] = true
        };

        var newGraphId = await AddVertexAndReturnIdAsync(label, tombstoneProperties, ct);
        if (newGraphId is null)
            return null;

        // Create supersedes edge from tombstone to previous version
        var edgeBindings = new Dictionary<string, object>
        {
            ["newVersionId"] = newGraphId,
            ["oldVersionId"] = previousVersionGraphId
        };
        await SubmitAsync<long>(Ops.CreateSupersedesEdge, Scripts.CreateSupersedesEdge, edgeBindings, ct);

        return (newGraphId, versionId);
    }

    public async Task<int> DeleteAllVersionsAsync(
        string label,
        string fhirId,
        CancellationToken ct = default)
    {
        // Count versions first
        var countBindings = new Dictionary<string, object>
        {
            ["lbl"] = label,
            ["fhirId"] = fhirId
        };

        var countResult = await SubmitAsync<long>(
            Ops.DeleteAllVersions,
            Scripts.CountVersions,
            countBindings,
            ct);

        var count = (int)countResult.FirstOrDefault();

        // Delete all versions
        var deleteBindings = new Dictionary<string, object>
        {
            ["lbl"] = label,
            ["fhirId"] = fhirId
        };

        await SubmitAsync<object>(Ops.DeleteAllVersions, Scripts.DeleteAllVersions, deleteBindings, ct);

        return count;
    }

    public async Task<bool> DeleteVersionAsync(
        string label,
        string fhirId,
        string versionId,
        CancellationToken ct = default)
    {
        // Check if version exists
        var version = await GetVersionAsync(label, fhirId, versionId, ct);
        if (version is null)
            return false;

        var bindings = new Dictionary<string, object>
        {
            ["lbl"] = label,
            ["fhirId"] = fhirId,
            ["versionId"] = versionId
        };

        await SubmitAsync<object>(Ops.DeleteVersion, Scripts.DeleteSingleVersion, bindings, ct);
        return true;
    }

    #endregion
}
