using System.Text.Json;
using BLL.Models;
using static BLL.Constants;

namespace BLL.Utils;

/// <summary>
/// Provides helper methods for creating FHIR-compliant API responses.
/// </summary>
public static class FhirResponseHelper
{
    /// <summary>
    /// Creates a FHIR OperationOutcome response object.
    /// </summary>
    public static object CreateOperationOutcome(string severity, string code, string? diagnostics)
    {
        return new
        {
            resourceType = ResourceTypes.OperationOutcome,
            issue = new[]
            {
                new
                {
                    severity,
                    code,
                    diagnostics
                }
            }
        };
    }

    /// <summary>
    /// Creates a FHIR history Bundle from a list of history entries.
    /// </summary>
    public static object CreateHistoryBundle(IReadOnlyList<HistoryEntry> entries, string selfUrl)
    {
        return new
        {
            resourceType = ResourceTypes.Bundle,
            type = BundleTypes.History,
            total = entries.Count,
            link = new[]
            {
                new { relation = Status.Self, url = selfUrl }
            },
            entry = entries.Select(e => new
            {
                fullUrl = e.FhirId != null
                    ? $"{selfUrl.Split("/_history")[0]}/{e.ResourceType}/{e.FhirId}/_history/{e.VersionId}"
                    : null,
                resource = e.Json != null && !e.IsDeleted
                    ? JsonSerializer.Deserialize<JsonElement>(e.Json)
                    : (JsonElement?)null,
                request = new
                {
                    method = e.IsDeleted ? HttpMethods.Delete : HttpMethods.Put,
                    url = $"{e.ResourceType}/{e.FhirId}"
                },
                response = new
                {
                    status = e.IsDeleted ? StatusCodes.NoContent : StatusCodes.Ok,
                    etag = $"W/\"{e.VersionId}\"",
                    lastModified = e.LastUpdated?.ToString("o")
                }
            }).ToArray()
        };
    }

    /// <summary>
    /// Creates a FHIR searchset Bundle from search results.
    /// </summary>
    public static object CreateSearchBundle(
        IReadOnlyList<FhirSearchResult> results, 
        long totalCount, 
        string selfUrl)
    {
        return new
        {
            resourceType = ResourceTypes.Bundle,
            type = BundleTypes.SearchSet,
            total = totalCount,
            link = new[]
            {
                new { relation = Status.Self, url = selfUrl }
            },
            entry = results.Where(r => r.Json != null).Select(r => new
            {
                fullUrl = $"{ExtractBaseUrl(selfUrl)}/api/fhir/{r.ResourceType}/{r.FhirId}",
                resource = JsonSerializer.Deserialize<JsonElement>(r.Json!),
                search = new { mode = Status.Match }
            }).ToArray()
        };
    }

    /// <summary>
    /// Parses search parameters from a query string.
    /// </summary>
    public static Dictionary<string, object> ParseSearchParameters(string queryString)
    {
        var result = new Dictionary<string, object>();
        var pairs = queryString.TrimStart('?').Split('&');

        foreach (var pair in pairs)
        {
            var parts = pair.Split('=', 2);
            if (parts.Length == 2)
            {
                result[Uri.UnescapeDataString(parts[0])] = Uri.UnescapeDataString(parts[1]);
            }
        }

        return result;
    }

    private static string ExtractBaseUrl(string url)
    {
        var uri = new Uri(url);
        return $"{uri.Scheme}://{uri.Host}{(uri.IsDefaultPort ? "" : $":{uri.Port}")}";
    }
}
