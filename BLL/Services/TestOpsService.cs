using DAL.Repositories;
using BLL.Models;
using BLL.Utils;
using System.Text.Json;
using static BLL.Constants;

namespace BLL.Services;

/// <summary>
/// Service for low-level graph testing and diagnostic operations.
/// Supports TestController.
/// </summary>
public class TestOpsService
{
    private readonly IGraphRepository _repo;

    public TestOpsService(IGraphRepository repo)
    {
        _repo = repo;
    }

    public async Task<OperationResult> PingAsync(CancellationToken ct = default)
    {
        var count = await _repo.CountVerticesAsync(ct);
        return OperationResult.Ok(new { status = Status.Ok, vertexCount = count });
    }

    public async Task<OperationResult> CreateVertexAsync(
        string label,
        IDictionary<string, object>? properties = null,
        CancellationToken ct = default)
    {
        var props = properties ?? new Dictionary<string, object>();
        var vertex = await _repo.AddVertexAsync(label, props, ct);
        return OperationResult.Created(new { id = vertex.Id, label = vertex.Label, properties = vertex.Properties });
    }

    public async Task<OperationResult> GetVertexAsync(
        string id,
        CancellationToken ct = default)
    {
        var vertex = await _repo.GetVertexByIdAsync(id, ct);
        if (vertex is null)
            return OperationResult.NotFound(new { error = "Not found" });

        return OperationResult.Ok(new { id = vertex.Id, label = vertex.Label, properties = vertex.Properties });
    }

    public async Task<OperationResult> UpdateVertexPropertiesAsync(
        string id,
        IDictionary<string, object> properties,
        CancellationToken ct = default)
    {
        if (properties.Count == 0)
            return OperationResult.BadRequest(new { error = "No properties provided" });

        var updated = await _repo.UpdateVertexPropertiesAsync(id, properties, ct);
        return updated
            ? OperationResult.NoContent()
            : OperationResult.NotFound(new { error = "Not found" });
    }

    public async Task<OperationResult> DeleteVertexAsync(string id, CancellationToken ct = default)
    {
        await _repo.DeleteVertexAsync(id, ct);
        return OperationResult.NoContent();
    }

    public async Task<OperationResult> WipeGraphAsync(CancellationToken ct = default)
    {
        var droppedCount = await _repo.DropAllAsync(ct);
        return OperationResult.Ok(new { status = Status.Wiped, droppedVertexCount = droppedCount });
    }

    public OperationResult ParseReferences(JsonElement payload)
    {
        var references = FhirReferenceHelper.EnumerateRelativeReferences(payload).ToList();
        return OperationResult.Ok(new
        {
            count = references.Count,
            references = references.Select(r => new
            {
                path = r.Path,
                edgeLabel = r.EdgeLabel,
                targetResourceType = r.TargetResourceType,
                targetFhirId = r.TargetFhirId
            })
        });
    }

    public async Task<OperationResult> LookupVertexAsync(
        string label,
        string propertyKey,
        object propertyValue,
        CancellationToken ct = default)
    {
        var graphId = await _repo.GetVertexIdByLabelAndPropertyAsync(label, propertyKey, propertyValue, ct);
        if (graphId is null)
            return OperationResult.NotFound(new { error = $"No vertex found with label={label} and {propertyKey}={propertyValue}" });

        var vertex = await _repo.GetVertexByIdAsync(graphId, ct);
        return OperationResult.Ok(new { graphId, label = vertex?.Label, properties = vertex?.Properties });
    }
}
