using System.Text.Json;
using BLL.Models;
using BLL.Utils;
using Microsoft.Extensions.Logging;
using static BLL.Constants;

namespace BLL.Services;

/// <summary>
/// Service for FHIR Bundle processing (batch and transaction).
/// </summary>
public class FhirBundleService
{
    private readonly ILogger<FhirBundleService> _logger;
    private readonly FhirPersistenceService _persistence;

    public FhirBundleService(
        ILogger<FhirBundleService> logger,
        FhirPersistenceService persistence)
    {
        _logger = logger;
        _persistence = persistence;
    }

    /// <summary>
    /// Processes a batch Bundle, executing each entry independently.
    /// </summary>
    public async Task<(bool ok, string? error, JsonDocument? responseBundle)> ProcessBatchAsync(
        JsonDocument bundleDoc,
        CancellationToken ct = default)
    {
        return await ProcessBundleAsync(bundleDoc, isTransaction: false, ct);
    }

    /// <summary>
    /// Processes a transaction Bundle, executing all entries atomically.
    /// </summary>
    public async Task<(bool ok, string? error, JsonDocument? responseBundle)> ProcessTransactionAsync(
        JsonDocument bundleDoc,
        CancellationToken ct = default)
    {
        return await ProcessBundleAsync(bundleDoc, isTransaction: true, ct);
    }

    private async Task<(bool ok, string? error, JsonDocument? responseBundle)> ProcessBundleAsync(
        JsonDocument bundleDoc,
        bool isTransaction,
        CancellationToken ct = default)
    {
        if (!bundleDoc.RootElement.TryGetProperty(BundleEntry.Entry, out var entries))
        {
            return (true, null, BundleHelper.CreateResponseBundle(
                isTransaction ? BundleTypes.TransactionResponse : BundleTypes.BatchResponse, []));
        }

        var responseEntries = new List<BundleResponseEntry>();
        var processedResources = new Dictionary<string, (string graphId, string fhirId)>();

        foreach (var entry in entries.EnumerateArray())
        {
            var (entryOk, entryError, response) = await ProcessBundleEntryAsync(entry, processedResources, ct);

            if (!entryOk && isTransaction)
                return (false, entryError, null);

            responseEntries.Add(response);
        }

        var responseBundle = BundleHelper.CreateResponseBundle(
            isTransaction ? BundleTypes.TransactionResponse : BundleTypes.BatchResponse,
            responseEntries);

        return (true, null, responseBundle);
    }

    private async Task<(bool ok, string? error, BundleResponseEntry response)> ProcessBundleEntryAsync(
        JsonElement entry,
        Dictionary<string, (string graphId, string fhirId)> processedResources,
        CancellationToken ct)
    {
        if (!entry.TryGetProperty(BundleEntry.Request, out var request))
        {
            return (false, "Entry missing request element",
                new BundleResponseEntry(StatusCodes.BadRequest, null, null, "Entry missing request element"));
        }

        var method = request.TryGetProperty(BundleEntry.Method, out var m) ? m.GetString() : null;
        var url = request.TryGetProperty(BundleEntry.Url, out var u) ? u.GetString() : null;

        if (string.IsNullOrEmpty(method) || string.IsNullOrEmpty(url))
        {
            return (false, "Invalid request method or url",
                new BundleResponseEntry(StatusCodes.BadRequest, null, null, "Invalid request method or url"));
        }

        try
        {
            return method.ToUpperInvariant() switch
            {
                HttpMethods.Get => await ProcessBundleGetAsync(url, ct),
                HttpMethods.Post => await ProcessBundlePostAsync(entry, url, processedResources, ct),
                HttpMethods.Put => await ProcessBundlePutAsync(entry, url, processedResources, ct),
                HttpMethods.Delete => await ProcessBundleDeleteAsync(url, ct),
                HttpMethods.Patch => await ProcessBundlePatchAsync(entry, url, ct),
                _ => (false, $"Unsupported method: {method}",
                    new BundleResponseEntry(StatusCodes.MethodNotAllowed, null, null, $"Unsupported method: {method}"))
            };
        }
        catch (Exception ex)
        {
            return (false, ex.Message,
                new BundleResponseEntry(StatusCodes.InternalServerError, null, null, ex.Message));
        }
    }

    private async Task<(bool ok, string? error, BundleResponseEntry response)> ProcessBundleGetAsync(
        string url, CancellationToken ct)
    {
        var parts = url.Split('?');
        var path = parts[0].Split('/');

        if (path.Length == 2)
        {
            var (ok, error, json, graphId) = await _persistence.GetByResourceTypeAndIdAsync(path[0], path[1], ct);
            if (!ok)
                return (false, error, new BundleResponseEntry(StatusCodes.NotFound, null, null, error));

            return (true, null, new BundleResponseEntry(StatusCodes.Ok, null, null, null, json));
        }

        return (false, "Search in bundle not supported",
            new BundleResponseEntry(StatusCodes.NotImplemented, null, null, "Search in bundle not supported"));
    }

    private async Task<(bool ok, string? error, BundleResponseEntry response)> ProcessBundlePostAsync(
        JsonElement entry, string url, Dictionary<string, (string graphId, string fhirId)> processedResources, CancellationToken ct)
    {
        if (!entry.TryGetProperty(BundleEntry.Resource, out var resource))
            return (false, "POST entry missing resource",
                new BundleResponseEntry(StatusCodes.BadRequest, null, null, "POST entry missing resource"));

        using var resourceDoc = JsonDocument.Parse(resource.GetRawText());
        var (ok, error, graphId, fhirId, _) = await _persistence.ValidateAndPersistAsync(resourceDoc, false, false, ct);

        if (!ok)
            return (false, error, new BundleResponseEntry(StatusCodes.UnprocessableEntity, null, null, error));

        if (entry.TryGetProperty(BundleEntry.FullUrl, out var fullUrl))
            processedResources[fullUrl.GetString()!] = (graphId!, fhirId!);

        var resourceType = url.TrimEnd('/');
        var location = $"{resourceType}/{fhirId}";
        return (true, null, new BundleResponseEntry(StatusCodes.Created, location, null, null));
    }

    private async Task<(bool ok, string? error, BundleResponseEntry response)> ProcessBundlePutAsync(
        JsonElement entry, string url, Dictionary<string, (string graphId, string fhirId)> processedResources, CancellationToken ct)
    {
        if (!entry.TryGetProperty(BundleEntry.Resource, out var resource))
            return (false, "PUT entry missing resource",
                new BundleResponseEntry(StatusCodes.BadRequest, null, null, "PUT entry missing resource"));

        using var resourceDoc = JsonDocument.Parse(resource.GetRawText());
        var (ok, error, graphId, fhirId, _) = await _persistence.ValidateAndPersistAsync(resourceDoc, false, false, ct);

        if (!ok)
            return (false, error, new BundleResponseEntry(StatusCodes.UnprocessableEntity, null, null, error));

        if (entry.TryGetProperty(BundleEntry.FullUrl, out var fullUrl))
            processedResources[fullUrl.GetString()!] = (graphId!, fhirId!);

        return (true, null, new BundleResponseEntry(StatusCodes.Ok, url, null, null));
    }

    private async Task<(bool ok, string? error, BundleResponseEntry response)> ProcessBundleDeleteAsync(
        string url, CancellationToken ct)
    {
        var path = url.Split('/');
        if (path.Length != 2)
            return (false, "Invalid DELETE url",
                new BundleResponseEntry(StatusCodes.BadRequest, null, null, "Invalid DELETE url"));

        var (ok, error) = await _persistence.DeleteByResourceTypeAndIdAsync(path[0], path[1], ct);

        return ok
            ? (true, null, new BundleResponseEntry(StatusCodes.NoContent, null, null, null))
            : (false, error, new BundleResponseEntry(StatusCodes.NotFound, null, null, error));
    }

    private async Task<(bool ok, string? error, BundleResponseEntry response)> ProcessBundlePatchAsync(
        JsonElement entry, string url, CancellationToken ct)
    {
        var path = url.Split('/');
        if (path.Length != 2)
            return (false, "Invalid PATCH url",
                new BundleResponseEntry(StatusCodes.BadRequest, null, null, "Invalid PATCH url"));

        if (!entry.TryGetProperty(BundleEntry.Resource, out var resource))
            return (false, "PATCH entry missing resource",
                new BundleResponseEntry(StatusCodes.BadRequest, null, null, "PATCH entry missing resource"));

        // For PATCH in bundle, we need to get the existing resource, apply the patch, and save
        var (getOk, getError, existingJson, _) = await _persistence.GetByResourceTypeAndIdAsync(path[0], path[1], ct);
        if (!getOk || existingJson is null)
            return (false, getError, new BundleResponseEntry(StatusCodes.NotFound, null, null, getError));

        try
        {
            using var existingDoc = JsonDocument.Parse(existingJson);
            using var patchDoc = JsonDocument.Parse(resource.GetRawText());

            var patchedJson = JsonPatchHelper.ApplyJsonPatch(existingDoc, patchDoc);
            if (patchedJson is null)
                return (false, "Failed to apply patch",
                    new BundleResponseEntry(StatusCodes.UnprocessableEntity, null, null, "Failed to apply patch"));

            using var patchedDoc = JsonDocument.Parse(patchedJson);
            var (ok, error, graphId, fhirId, _) = await _persistence.ValidateAndPersistAsync(patchedDoc, false, false, ct);

            return ok
                ? (true, null, new BundleResponseEntry(StatusCodes.Ok, url, null, null))
                : (false, error, new BundleResponseEntry(StatusCodes.UnprocessableEntity, null, null, error));
        }
        catch (Exception ex)
        {
            return (false, ex.Message,
                new BundleResponseEntry(StatusCodes.UnprocessableEntity, null, null, ex.Message));
        }
    }
}
