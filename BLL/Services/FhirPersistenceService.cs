using System.Text.Json;
using BLL.Models;
using DAL.Repositories;
using Microsoft.Extensions.Logging;
using static BLL.Constants;

namespace BLL.Services;

/// <summary>
/// Service for core FHIR resource persistence operations (CRUD).
/// </summary>
public class FhirPersistenceService
{
    private readonly IGraphRepository _repo;
    private readonly ILogger<FhirPersistenceService> _logger;
    private readonly FhirValidationService _validation;
    private readonly FhirReferenceService _references;

    public FhirPersistenceService(
        IGraphRepository repo,
        ILogger<FhirPersistenceService> logger,
        FhirValidationService validation,
        FhirReferenceService references)
    {
        _repo = repo;
        _logger = logger;
        _validation = validation;
        _references = references;
    }

    /// <summary>
    /// Validates the given FHIR JSON and persists it to the graph.
    /// </summary>
    public async Task<(bool ok, string? error, string? graphId, string? fhirId, int? materializedReferenceCount)>
        ValidateAndPersistAsync(
            JsonDocument json,
            bool materializeReferences = false,
            bool allowPlaceholderTargets = false,
            CancellationToken ct = default)
    {
        var (extracted, extractError, resourceType, fhirId) = _validation.ExtractResourceInfo(json);
        if (!extracted)
            return (false, extractError, null, null, null);

        var (valid, error) = _validation.Validate(json);
        if (!valid)
            return (false, error, null, null, null);

        var props = new Dictionary<string, object>
        {
            [Properties.ResourceType] = resourceType,
            [Properties.Json] = json.RootElement.GetRawText()
        };

        if (!string.IsNullOrWhiteSpace(fhirId))
            props[Properties.Id] = fhirId!;

        string? graphId;

        if (!string.IsNullOrWhiteSpace(fhirId))
        {
            graphId = await _repo.UpsertVertexAndReturnIdAsync(
                resourceType, Properties.Id, fhirId!, props, ct);
        }
        else
        {
            graphId = await _repo.AddVertexAndReturnIdAsync(resourceType, props, ct);
        }

        if (materializeReferences && graphId is not null)
        {
            var count = await _references.TryMaterializeReferencesAsync(
                graphId, json.RootElement, allowPlaceholderTargets, ct);
            return (true, null, graphId, fhirId, count);
        }

        return (true, null, graphId, fhirId, null);
    }

    /// <summary>
    /// Gets a FHIR resource by resourceType and FHIR id.
    /// </summary>
    public async Task<(bool ok, string? error, string? json, string? graphId)> GetByResourceTypeAndIdAsync(
        string resourceType,
        string fhirId,
        CancellationToken ct = default)
    {
        var vertex = await _repo.GetVertexByLabelAndPropertyAsync(resourceType, Properties.Id, fhirId, ct);
        if (vertex is null)
            return (false, $"{resourceType}/{fhirId} not found", null, null);

        var json = vertex.Properties?.TryGetValue(Properties.Json, out var j) == true ? j?.ToString() : null;
        if (json is null)
            return (false, "Resource has no JSON payload", null, vertex.Id);

        return (true, null, json, vertex.Id);
    }

    /// <summary>
    /// Deletes a FHIR resource by resourceType and FHIR id.
    /// </summary>
    public async Task<(bool ok, string? error)> DeleteByResourceTypeAndIdAsync(
        string resourceType,
        string fhirId,
        CancellationToken ct = default)
    {
        var graphId = await _repo.GetVertexIdByLabelAndPropertyAsync(resourceType, Properties.Id, fhirId, ct);
        if (graphId is null)
            return (false, $"{resourceType}/{fhirId} not found");

        var deleted = await _repo.DeleteVertexAsync(graphId, ct);
        return deleted ? (true, null) : (false, "Delete failed");
    }

    /// <summary>
    /// Searches for FHIR resources by resourceType with optional filters.
    /// </summary>
    public async Task<(bool ok, string? error, IReadOnlyList<FhirSearchResult> results, long totalCount)> SearchAsync(
        string resourceType,
        IDictionary<string, object>? filters = null,
        int limit = 100,
        int offset = 0,
        CancellationToken ct = default)
    {
        try
        {
            var totalCount = await _repo.CountVerticesByLabelAsync(resourceType, filters, ct);
            var vertices = await _repo.GetVerticesByLabelAsync(resourceType, filters, limit, offset, ct);

            var results = new List<FhirSearchResult>();
            foreach (var v in vertices)
            {
                var json = v.Properties?.TryGetValue(Properties.Json, out var j) == true ? j?.ToString() : null;
                var fhirId = v.Properties?.TryGetValue(Properties.Id, out var id) == true ? id?.ToString() : null;
                var isPlaceholder = v.Properties?.TryGetValue(Properties.IsPlaceholder, out var ph) == true &&
                                    (ph?.ToString()?.Equals(BooleanStrings.True, StringComparison.OrdinalIgnoreCase) == true || ph is true);

                results.Add(new FhirSearchResult(v.Id, fhirId, resourceType, json, isPlaceholder));
            }

            return (true, null, results, totalCount);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Search failed for resourceType={ResourceType}", resourceType);
            return (false, ex.Message, [], 0);
        }
    }

    /// <summary>
    /// Searches across all resource types.
    /// </summary>
    public async Task<(bool ok, string? error, IReadOnlyList<FhirSearchResult> results, long totalCount)> SearchAllTypesAsync(
        IReadOnlyList<string>? resourceTypes,
        IDictionary<string, object>? filters = null,
        int limit = 100,
        int offset = 0,
        CancellationToken ct = default)
    {
        var allResults = new List<FhirSearchResult>();
        long totalCount = 0;

        var typesToSearch = resourceTypes?.Count > 0
            ? resourceTypes
            : _validation.GetSupportedResourceTypes();

        foreach (var resourceType in typesToSearch)
        {
            var (ok, error, results, count) = await SearchAsync(resourceType, filters, limit, offset, ct);
            if (ok)
            {
                allResults.AddRange(results);
                totalCount += count;
            }
        }

        var limitedResults = allResults.Take(limit).ToList();
        return (true, null, limitedResults, totalCount);
    }
}
