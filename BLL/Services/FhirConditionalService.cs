using System.Text.Json;
using BLL.Models;
using BLL.Utils;
using Microsoft.Extensions.Logging;

namespace BLL.Services;

/// <summary>
/// Service for FHIR conditional operations (create, update, delete, patch with search criteria).
/// </summary>
public class FhirConditionalService
{
    private readonly ILogger<FhirConditionalService> _logger;
    private readonly FhirPersistenceService _persistence;
    private readonly FhirValidationService _validation;

    public FhirConditionalService(
        ILogger<FhirConditionalService> logger,
        FhirPersistenceService persistence,
        FhirValidationService validation)
    {
        _logger = logger;
        _persistence = persistence;
        _validation = validation;
    }

    /// <summary>
    /// Conditional create: creates a resource only if no match is found.
    /// </summary>
    public async Task<(bool ok, string? error, string? graphId, string? fhirId, bool created, int? materializedCount)>
        ConditionalCreateAsync(
            JsonDocument json,
            string resourceType,
            IDictionary<string, object> searchCriteria,
            bool materializeReferences = false,
            bool allowPlaceholderTargets = false,
            CancellationToken ct = default)
    {
        var (searchOk, searchError, results, totalCount) = await _persistence.SearchAsync(
            resourceType, searchCriteria, limit: 2, offset: 0, ct);

        if (!searchOk)
            return (false, searchError, null, null, false, null);

        if (totalCount > 1)
            return (false, "Multiple matches found - criteria not selective enough (412 Precondition Failed)", null, null, false, null);

        if (totalCount == 1)
        {
            var existing = results[0];
            return (true, null, existing.GraphId, existing.FhirId, false, null);
        }

        var (ok, error, graphId, fhirId, materializedCount) = await _persistence.ValidateAndPersistAsync(
            json, materializeReferences, allowPlaceholderTargets, ct);
        return (ok, error, graphId, fhirId, ok, materializedCount);
    }

    /// <summary>
    /// Conditional update: updates a resource based on search criteria.
    /// </summary>
    public async Task<(bool ok, string? error, string? graphId, string? fhirId, bool created, int? materializedCount)>
        ConditionalUpdateAsync(
            JsonDocument json,
            string resourceType,
            IDictionary<string, object> searchCriteria,
            bool materializeReferences = false,
            bool allowPlaceholderTargets = false,
            CancellationToken ct = default)
    {
        var (searchOk, searchError, results, totalCount) = await _persistence.SearchAsync(
            resourceType, searchCriteria, limit: 2, offset: 0, ct);

        if (!searchOk)
            return (false, searchError, null, null, false, null);

        if (totalCount > 1)
            return (false, "Multiple matches found - criteria not selective enough (412 Precondition Failed)", null, null, false, null);

        var (extracted, extractError, _, providedFhirId) = _validation.ExtractResourceInfo(json);
        if (!extracted)
            return (false, extractError, null, null, false, null);

        if (totalCount == 1)
        {
            var existing = results[0];

            if (!string.IsNullOrEmpty(providedFhirId) && providedFhirId != existing.FhirId)
                return (false, $"Resource id mismatch: provided {providedFhirId} but found {existing.FhirId}", null, null, false, null);

            var modifiedJson = FhirResourceHelper.EnsureResourceId(json, existing.FhirId!);
            using var modifiedDoc = JsonDocument.Parse(modifiedJson);

            var (ok, error, graphId, fhirId, materializedCount) = await _persistence.ValidateAndPersistAsync(
                modifiedDoc, materializeReferences, allowPlaceholderTargets, ct);
            return (ok, error, graphId, fhirId, false, materializedCount);
        }

        if (string.IsNullOrEmpty(providedFhirId))
            return (false, "No matches and no id provided - cannot create", null, null, false, null);

        var (createOk, createError, createGraphId, createFhirId, createMaterializedCount) =
            await _persistence.ValidateAndPersistAsync(json, materializeReferences, allowPlaceholderTargets, ct);
        return (createOk, createError, createGraphId, createFhirId, createOk, createMaterializedCount);
    }

    /// <summary>
    /// Conditional delete: deletes resources matching the search criteria.
    /// </summary>
    public async Task<(bool ok, string? error, int deletedCount)> ConditionalDeleteAsync(
        string resourceType,
        IDictionary<string, object> searchCriteria,
        bool allowMultiple = false,
        CancellationToken ct = default)
    {
        var (searchOk, searchError, results, totalCount) = await _persistence.SearchAsync(
            resourceType, searchCriteria, limit: allowMultiple ? 1000 : 2, offset: 0, ct);

        if (!searchOk)
            return (false, searchError, 0);

        if (totalCount == 0)
            return (true, null, 0);

        if (totalCount > 1 && !allowMultiple)
            return (false, "Multiple matches found - criteria not selective enough (412 Precondition Failed)", 0);

        int deletedCount = 0;
        foreach (var result in results)
        {
            var (ok, _) = await _persistence.DeleteByResourceTypeAndIdAsync(
                resourceType, result.FhirId!, ct);
            if (ok) deletedCount++;
        }

        return (true, null, deletedCount);
    }

    /// <summary>
    /// Applies a JSON Patch to a FHIR resource.
    /// </summary>
    public async Task<(bool ok, string? error, string? graphId, string? fhirId)> PatchAsync(
        string resourceType,
        string fhirId,
        JsonDocument patchDocument,
        CancellationToken ct = default)
    {
        var (getOk, getError, existingJson, graphId) = await _persistence.GetByResourceTypeAndIdAsync(
            resourceType, fhirId, ct);
        if (!getOk || existingJson is null)
            return (false, getError ?? "Resource not found", null, null);

        try
        {
            using var existingDoc = JsonDocument.Parse(existingJson);

            var patchedJson = JsonPatchHelper.ApplyJsonPatch(existingDoc, patchDocument);
            if (patchedJson is null)
                return (false, "Failed to apply patch operations", null, null);

            using var patchedDoc = JsonDocument.Parse(patchedJson);
            var (ok, error, newGraphId, newFhirId, _) = await _persistence.ValidateAndPersistAsync(
                patchedDoc, false, false, ct);

            return (ok, error, newGraphId, newFhirId);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Patch failed for {ResourceType}/{FhirId}", resourceType, fhirId);
            return (false, $"Patch failed: {ex.Message}", null, null);
        }
    }

    /// <summary>
    /// Conditional patch: patches a resource based on search criteria.
    /// </summary>
    public async Task<(bool ok, string? error, string? graphId, string? fhirId)> ConditionalPatchAsync(
        string resourceType,
        IDictionary<string, object> searchCriteria,
        JsonDocument patchDocument,
        CancellationToken ct = default)
    {
        var (searchOk, searchError, results, totalCount) = await _persistence.SearchAsync(
            resourceType, searchCriteria, limit: 2, offset: 0, ct);

        if (!searchOk)
            return (false, searchError, null, null);

        if (totalCount == 0)
            return (false, "No matches found (404 Not Found)", null, null);

        if (totalCount > 1)
            return (false, "Multiple matches found - criteria not selective enough (412 Precondition Failed)", null, null);

        var existing = results[0];
        return await PatchAsync(resourceType, existing.FhirId!, patchDocument, ct);
    }
}
