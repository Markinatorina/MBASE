using System.Text.Json;
using BLL.Utils;
using DAL.Repositories;
using Microsoft.Extensions.Logging;
using static BLL.Constants;

namespace BLL.Services;

/// <summary>
/// Service for FHIR reference materialization (creating graph edges from FHIR references).
/// </summary>
public class FhirReferenceService
{
    private readonly IGraphRepository _repo;
    private readonly ILogger<FhirReferenceService> _logger;

    public FhirReferenceService(IGraphRepository repo, ILogger<FhirReferenceService> logger)
    {
        _repo = repo;
        _logger = logger;
    }

    /// <summary>
    /// Materializes FHIR references as graph edges.
    /// </summary>
    public async Task<int> TryMaterializeReferencesAsync(
        string sourceVertexId,
        JsonElement sourceResource,
        bool allowPlaceholderTargets,
        CancellationToken ct)
    {
        try
        {
            var materialized = 0;
            var references = FhirReferenceHelper.EnumerateRelativeReferences(sourceResource).ToList();

            _logger.LogDebug(
                "Found {ReferenceCount} relative references in resource for sourceVertexId={SourceVertexId}",
                references.Count, sourceVertexId);

            foreach (var reference in references)
            {
                _logger.LogDebug(
                    "Processing reference: path={Path} target={Target}",
                    reference.Path, $"{reference.TargetResourceType}/{reference.TargetFhirId}");

                string? toVertex;

                if (!allowPlaceholderTargets)
                {
                    toVertex = await TryResolveExistingTargetVertexIdAsync(
                        reference.TargetResourceType, reference.TargetFhirId, ct);

                    if (string.IsNullOrWhiteSpace(toVertex))
                    {
                        _logger.LogDebug(
                            "FHIR reference unresolved; skipping edge. sourceVertexId={SourceVertexId} path={Path} target={Target}",
                            sourceVertexId, reference.Path, $"{reference.TargetResourceType}/{reference.TargetFhirId}");
                        continue;
                    }
                }
                else
                {
                    toVertex = await EnsurePlaceholderTargetVertexIdAsync(
                        reference.TargetResourceType, reference.TargetFhirId, ct);

                    if (string.IsNullOrWhiteSpace(toVertex))
                    {
                        _logger.LogWarning(
                            "Failed to create/resolve placeholder target. sourceVertexId={SourceVertexId} path={Path} target={Target}",
                            sourceVertexId, reference.Path, $"{reference.TargetResourceType}/{reference.TargetFhirId}");
                        continue;
                    }
                }

                var edgeProps = new Dictionary<string, object>
                {
                    [EdgeProperties.Path] = reference.Path,
                    [EdgeProperties.TargetResourceType] = reference.TargetResourceType,
                    [EdgeProperties.TargetFhirId] = reference.TargetFhirId
                };

                var exists = await _repo.EdgeExistsAsync(
                    label: reference.EdgeLabel,
                    outVertexId: sourceVertexId,
                    inVertexId: toVertex!,
                    ct);

                if (exists)
                {
                    _logger.LogDebug(
                        "Edge already exists; skipping. label={Label} from={From} to={To}",
                        reference.EdgeLabel, sourceVertexId, toVertex);
                    continue;
                }

                await _repo.AddEdgeAsync(reference.EdgeLabel, sourceVertexId, toVertex!, edgeProps, ct);

                _logger.LogDebug(
                    "Created edge: label={Label} from={From} to={To}",
                    reference.EdgeLabel, sourceVertexId, toVertex);

                materialized++;
            }

            return materialized;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Exception during reference materialization for sourceVertexId={SourceVertexId}",
                sourceVertexId);
            return 0;
        }
    }

    /// <summary>
    /// Tries to resolve an existing target vertex by resourceType and FHIR id.
    /// </summary>
    public async Task<string?> TryResolveExistingTargetVertexIdAsync(
        string targetResourceType,
        string targetFhirId,
        CancellationToken ct)
    {
        try
        {
            return await _repo.GetVertexIdByLabelAndPropertyAsync(
                targetResourceType, Properties.Id, targetFhirId, ct);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex,
                "Failed to resolve target vertex. resourceType={ResourceType} fhirId={FhirId}",
                targetResourceType, targetFhirId);
            return null;
        }
    }

    /// <summary>
    /// Ensures a placeholder target vertex exists, creating one if necessary.
    /// </summary>
    public async Task<string?> EnsurePlaceholderTargetVertexIdAsync(
        string targetResourceType,
        string targetFhirId,
        CancellationToken ct)
    {
        try
        {
            var props = new Dictionary<string, object>
            {
                [Properties.ResourceType] = targetResourceType,
                [Properties.Id] = targetFhirId,
                [Properties.IsPlaceholder] = true
            };

            return await _repo.UpsertVertexAndReturnIdAsync(
                targetResourceType, Properties.Id, targetFhirId, props, ct);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex,
                "Failed to create placeholder vertex. resourceType={ResourceType} fhirId={FhirId}",
                targetResourceType, targetFhirId);
            return null;
        }
    }
}
