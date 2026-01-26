using DAL.Repositories;
using BLL.Models;
using System.Text.Json;
using static BLL.Constants;

namespace BLL.Services;

/// <summary>
/// Service for graph traversal and edge operations.
/// Supports GraphController.
/// </summary>
public class GraphOpsService
{
    private readonly IGraphRepository _repo;

    public GraphOpsService(IGraphRepository repo)
    {
        _repo = repo;
    }

    /// <summary>
    /// Gets all outgoing references from a FHIR resource.
    /// </summary>
    public async Task<OperationResult> GetOutgoingReferencesAsync(
        string resourceType,
        string fhirId,
        CancellationToken ct = default)
    {
        var graphId = await _repo.GetVertexIdByLabelAndPropertyAsync(resourceType, Properties.Id, fhirId, ct);
        if (graphId is null)
            return OperationResult.NotFound(new { error = $"{resourceType}/{fhirId} not found" });

        var edges = await _repo.GetEdgesForVertexAsync(graphId, ct);
        var outgoing = edges
            .Where(e => e.Direction == EdgeDirection.Out && e.Label.StartsWith(EdgeLabels.FhirReferencePrefix, StringComparison.Ordinal))
            .ToList();

        var results = new List<object>();
        foreach (var edge in outgoing)
        {
            var targetVertex = await _repo.GetVertexByIdAsync(edge.TargetVertexId, ct);
            var targetResourceType = edge.Properties.TryGetValue(EdgeProperties.TargetResourceType, out var trt) ? trt?.ToString() : targetVertex?.Label;
            var targetFhirId = edge.Properties.TryGetValue(EdgeProperties.TargetFhirId, out var tfid) ? tfid?.ToString() :
                               targetVertex?.Properties?.TryGetValue(Properties.Id, out var tid) == true ? tid?.ToString() : null;
            var path = edge.Properties.TryGetValue(EdgeProperties.Path, out var p) ? p?.ToString() : null;

            results.Add(new
            {
                path,
                targetResourceType,
                targetFhirId,
                targetGraphId = edge.TargetVertexId
            });
        }

        return OperationResult.Ok(new { resourceType, fhirId, references = results });
    }

    /// <summary>
    /// Gets all incoming references to a FHIR resource.
    /// </summary>
    public async Task<OperationResult> GetIncomingReferencesAsync(
        string resourceType,
        string fhirId,
        CancellationToken ct = default)
    {
        var graphId = await _repo.GetVertexIdByLabelAndPropertyAsync(resourceType, Properties.Id, fhirId, ct);
        if (graphId is null)
            return OperationResult.NotFound(new { error = $"{resourceType}/{fhirId} not found" });

        var edges = await _repo.GetEdgesForVertexAsync(graphId, ct);
        var incoming = edges
            .Where(e => e.Direction == EdgeDirection.In && e.Label.StartsWith(EdgeLabels.FhirReferencePrefix, StringComparison.Ordinal))
            .ToList();

        var results = new List<object>();
        foreach (var edge in incoming)
        {
            var sourceVertex = await _repo.GetVertexByIdAsync(edge.TargetVertexId, ct);
            results.Add(new
            {
                sourceResourceType = sourceVertex?.Label,
                sourceFhirId = sourceVertex?.Properties?.TryGetValue(Properties.Id, out var sid) == true ? sid?.ToString() : null,
                sourceGraphId = edge.TargetVertexId,
                referencePath = edge.Properties.TryGetValue(EdgeProperties.Path, out var p) ? p?.ToString() : null
            });
        }

        return OperationResult.Ok(new { resourceType, fhirId, referrers = results });
    }

    /// <summary>
    /// Gets all resources within N hops of a FHIR resource.
    /// </summary>
    public async Task<OperationResult> TraverseFromResourceAsync(
        string resourceType,
        string fhirId,
        int maxHops = 2,
        int limit = 100,
        CancellationToken ct = default)
    {
        var graphId = await _repo.GetVertexIdByLabelAndPropertyAsync(resourceType, Properties.Id, fhirId, ct);
        if (graphId is null)
            return OperationResult.NotFound(new { error = $"{resourceType}/{fhirId} not found" });

        var vertices = await _repo.TraverseAsync(graphId, maxHops, edgeLabelFilter: null, limit, ct);

        var results = vertices.Select(v => new
        {
            graphId = v.Id,
            fhirId = v.Properties?.TryGetValue(Properties.Id, out var id) == true ? id?.ToString() : null,
            resourceType = v.Label,
            isPlaceholder = v.Properties?.TryGetValue(Properties.IsPlaceholder, out var ph) == true &&
                            (ph?.ToString()?.Equals(BooleanStrings.True, StringComparison.OrdinalIgnoreCase) == true || ph is true),
            resource = v.Properties?.TryGetValue(Properties.Json, out var j) == true && j != null
                ? JsonSerializer.Deserialize<JsonElement>(j.ToString()!)
                : (JsonElement?)null
        }).ToList();

        return OperationResult.Ok(new
        {
            source = new { resourceType, fhirId },
            maxHops,
            totalFound = results.Count,
            resources = results
        });
    }

    /// <summary>
    /// Gets JSON by graph vertex id.
    /// </summary>
    public async Task<OperationResult> GetJsonByGraphIdAsync(
        string id,
        CancellationToken ct = default)
    {
        var v = await _repo.GetVertexByIdAsync(id, ct);
        if (v == null)
            return OperationResult.NotFound(new { error = "Not found" });

        var raw = v.Properties?
            .FirstOrDefault(p => p.Key == Properties.Json)
            .Value?
            .ToString();

        if (raw is null)
            return OperationResult.NotFound(new { error = "Stored vertex has no json payload" });

        return new OperationResult(true, 200, raw);
    }

    /// <summary>
    /// Gets edges for a vertex.
    /// </summary>
    public async Task<OperationResult> GetEdgesForVertexAsync(
        string id,
        CancellationToken ct = default)
    {
        var v = await _repo.GetVertexByIdAsync(id, ct);
        if (v is null)
            return OperationResult.NotFound(new { error = "Vertex not found" });

        var edges = await _repo.GetEdgesForVertexAsync(id, ct);

        var response = edges.Select(e => new
        {
            direction = e.Direction,
            label = e.Label,
            path = e.Properties.TryGetValue(EdgeProperties.Path, out var path) ? path?.ToString() : null,
            targetVertexId = e.TargetVertexId,
            targetResourceType = e.Properties.TryGetValue(EdgeProperties.TargetResourceType, out var trt) ? trt?.ToString() : null,
            targetFhirId = e.Properties.TryGetValue(EdgeProperties.TargetFhirId, out var tfid) ? tfid?.ToString() : null
        }).ToList();

        return OperationResult.Ok(new
        {
            vertexId = id,
            vertexLabel = v.Label,
            edgeCount = response.Count,
            edges = response
        });
    }

    /// <summary>
    /// Updates vertex by graph id.
    /// </summary>
    public async Task<OperationResult> UpdateByGraphIdAsync(
        string id,
        IDictionary<string, object> properties,
        CancellationToken ct = default)
    {
        var ok = await _repo.UpdateVertexPropertiesAsync(id, properties, ct);
        return ok
            ? OperationResult.Ok(new { graphId = id })
            : OperationResult.BadRequest(new { error = "Not found" });
    }

    /// <summary>
    /// Deletes vertex by graph id.
    /// </summary>
    public async Task<OperationResult> DeleteByGraphIdAsync(string id, CancellationToken ct = default)
    {
        var ok = await _repo.DeleteVertexAsync(id, ct);
        return ok
            ? OperationResult.NoContent()
            : OperationResult.NotFound(new { error = "Delete failed" });
    }

    /// <summary>
    /// Creates an edge between two vertices.
    /// </summary>
    public async Task<OperationResult> CreateEdgeAsync(
        CreateEdgeRequest request,
        CancellationToken ct = default)
    {
        var edge = await _repo.AddEdgeByPropertyAsync(
            request.Label,
            request.OutLabel,
            request.OutKey,
            request.OutValue,
            request.InLabel,
            request.InKey,
            request.InValue,
            request.Properties ?? new Dictionary<string, object>(),
            ct);

        return edge == null
            ? OperationResult.BadRequest(new { error = "Vertices not found or edge creation failed" })
            : OperationResult.Ok(new { label = edge.Label, created = true });
    }
}
