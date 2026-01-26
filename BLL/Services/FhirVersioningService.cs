using System.Text.Json;
using BLL.Models;
using BLL.Utils;
using DAL.Repositories;
using Microsoft.Extensions.Logging;
using static BLL.Constants;

namespace BLL.Services;

/// <summary>
/// Service for FHIR versioning operations (history, vread, soft delete).
/// </summary>
public class FhirVersioningService
{
    private readonly IGraphRepository _repo;
    private readonly ILogger<FhirVersioningService> _logger;
    private readonly FhirValidationService _validation;
    private readonly FhirReferenceService _references;

    public FhirVersioningService(
        IGraphRepository repo,
        ILogger<FhirVersioningService> logger,
        FhirValidationService validation,
        FhirReferenceService references)
    {
        _repo = repo;
        _logger = logger;
        _validation = validation;
        _references = references;
    }

    /// <summary>
    /// Gets a specific version of a resource (vread operation).
    /// </summary>
    public async Task<(bool ok, string? error, string? json, string? graphId, VersionInfo? versionInfo)>
        GetVersionAsync(
            string resourceType,
            string fhirId,
            string versionId,
            CancellationToken ct = default)
    {
        var vertex = await _repo.GetVersionAsync(resourceType, fhirId, versionId, ct);
        if (vertex is null)
            return (false, $"{resourceType}/{fhirId}/_history/{versionId} not found", null, null, null);

        var json = vertex.Properties?.TryGetValue(Properties.Json, out var j) == true ? j?.ToString() : null;
        var isDeleted = vertex.Properties?.TryGetValue(Properties.IsDeleted, out var del) == true &&
                        (del?.ToString()?.Equals(BooleanStrings.True, StringComparison.OrdinalIgnoreCase) == true || del is true);

        var versionInfo = ExtractVersionInfo(vertex);

        if (isDeleted)
            return (false, $"{resourceType}/{fhirId} was deleted at version {versionId}", null, vertex.Id, versionInfo);

        if (json is null)
            return (false, "Version has no JSON payload", null, vertex.Id, versionInfo);

        return (true, null, json, vertex.Id, versionInfo);
    }

    /// <summary>
    /// Gets the version history of a specific resource (instance history).
    /// </summary>
    public async Task<(bool ok, string? error, IReadOnlyList<HistoryEntry> entries)>
        GetInstanceHistoryAsync(
            string resourceType,
            string fhirId,
            int limit = 100,
            CancellationToken ct = default)
    {
        var vertices = await _repo.GetVersionHistoryAsync(resourceType, fhirId, limit, ct);

        if (vertices.Count == 0)
            return (false, $"{resourceType}/{fhirId} not found", []);

        var entries = vertices.Select(v => ToHistoryEntry(resourceType, v)).ToList();
        return (true, null, entries);
    }

    /// <summary>
    /// Gets the version history of all resources of a type (type history).
    /// </summary>
    public async Task<(bool ok, string? error, IReadOnlyList<HistoryEntry> entries)>
        GetTypeHistoryAsync(
            string resourceType,
            int limit = 100,
            DateTime? since = null,
            CancellationToken ct = default)
    {
        var vertices = await _repo.GetTypeHistoryAsync(resourceType, limit, since, ct);
        var entries = vertices.Select(v => ToHistoryEntry(resourceType, v)).ToList();
        return (true, null, entries);
    }

    /// <summary>
    /// Gets the version history of all resources (system history).
    /// </summary>
    public async Task<(bool ok, string? error, IReadOnlyList<HistoryEntry> entries)>
        GetSystemHistoryAsync(
            int limit = 100,
            DateTime? since = null,
            CancellationToken ct = default)
    {
        var resourceTypes = _validation.GetSupportedResourceTypes();
        var allEntries = new List<HistoryEntry>();

        foreach (var resourceType in resourceTypes)
        {
            var vertices = await _repo.GetTypeHistoryAsync(resourceType, limit, since, ct);
            allEntries.AddRange(vertices.Select(v => ToHistoryEntry(resourceType, v)));
        }

        var sorted = allEntries
            .OrderByDescending(e => e.LastUpdated)
            .Take(limit)
            .ToList();

        return (true, null, sorted);
    }

    /// <summary>
    /// Creates a versioned resource with proper version tracking.
    /// </summary>
    public async Task<(bool ok, string? error, string? graphId, string? fhirId, string? versionId, int? materializedCount)>
        CreateVersionedResourceAsync(
            JsonDocument json,
            bool materializeReferences = false,
            bool allowPlaceholderTargets = false,
            CancellationToken ct = default)
    {
        var (extracted, extractError, resourceType, fhirId) = _validation.ExtractResourceInfo(json);
        if (!extracted)
            return (false, extractError, null, null, null, null);

        var (valid, error) = _validation.Validate(json);
        if (!valid)
            return (false, error, null, null, null, null);

        var props = new Dictionary<string, object>
        {
            [Properties.ResourceType] = resourceType,
            [Properties.Json] = json.RootElement.GetRawText()
        };

        if (string.IsNullOrWhiteSpace(fhirId))
        {
            fhirId = Guid.NewGuid().ToString();
            props[Properties.Id] = fhirId;
        }

        try
        {
            var (graphId, versionId) = await _repo.CreateVersionedVertexAsync(resourceType, fhirId, props, ct);

            int? materializedCount = null;
            if (materializeReferences)
            {
                materializedCount = await _references.TryMaterializeReferencesAsync(
                    graphId, json.RootElement, allowPlaceholderTargets, ct);
            }

            return (true, null, graphId, fhirId, versionId, materializedCount);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to create versioned resource: {ResourceType}/{FhirId}", resourceType, fhirId);
            return (false, $"Failed to create versioned resource: {ex.Message}", null, null, null, null);
        }
    }

    /// <summary>
    /// Soft deletes a resource by creating a tombstone version.
    /// </summary>
    public async Task<(bool ok, string? error, string? versionId)> SoftDeleteAsync(
        string resourceType,
        string fhirId,
        CancellationToken ct = default)
    {
        var result = await _repo.CreateTombstoneAsync(resourceType, fhirId, ct);
        if (result is null)
            return (false, $"{resourceType}/{fhirId} not found or already deleted", null);

        return (true, null, result.Value.versionId);
    }

    /// <summary>
    /// Permanently deletes all versions of a resource.
    /// </summary>
    public async Task<(bool ok, string? error, int deletedCount)> DeleteHistoryAsync(
        string resourceType,
        string fhirId,
        CancellationToken ct = default)
    {
        var count = await _repo.DeleteAllVersionsAsync(resourceType, fhirId, ct);
        if (count == 0)
            return (false, $"{resourceType}/{fhirId} not found", 0);

        return (true, null, count);
    }

    /// <summary>
    /// Permanently deletes a specific version of a resource.
    /// </summary>
    public async Task<(bool ok, string? error)> DeleteVersionAsync(
        string resourceType,
        string fhirId,
        string versionId,
        CancellationToken ct = default)
    {
        var deleted = await _repo.DeleteVersionAsync(resourceType, fhirId, versionId, ct);
        if (!deleted)
            return (false, $"{resourceType}/{fhirId}/_history/{versionId} not found");

        return (true, null);
    }

    private static VersionInfo ExtractVersionInfo(GraphVertex vertex)
    {
        return FhirResourceHelper.ExtractVersionInfo(vertex.Properties);
    }

    private static HistoryEntry ToHistoryEntry(string resourceType, GraphVertex vertex)
    {
        return FhirResourceHelper.ToHistoryEntry(resourceType, vertex.Id, vertex.Properties);
    }
}
