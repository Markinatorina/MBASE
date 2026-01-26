using System.Text.Json;
using System.Text.Json.Nodes;
using BLL.Models;
using static BLL.Constants;

namespace BLL.Utils;

/// <summary>
/// Provides helper methods for working with FHIR resources.
/// </summary>
public static class FhirResourceHelper
{
    /// <summary>
    /// Ensures that a FHIR resource has the specified id.
    /// </summary>
    /// <param name="doc">The JSON document containing the FHIR resource.</param>
    /// <param name="fhirId">The FHIR id to set.</param>
    /// <returns>The JSON string with the id set.</returns>
    public static string EnsureResourceId(JsonDocument doc, string fhirId)
    {
        var node = JsonNode.Parse(doc.RootElement.GetRawText());
        if (node is JsonObject obj)
        {
            obj[Properties.Id] = fhirId;
            return obj.ToJsonString();
        }
        return doc.RootElement.GetRawText();
    }

    /// <summary>
    /// Extracts version information from a graph vertex.
    /// </summary>
    public static VersionInfo ExtractVersionInfo(IDictionary<string, object>? properties)
    {
        var versionId = properties?.TryGetValue(Properties.VersionId, out var vid) == true ? vid?.ToString() : null;
        var lastUpdatedStr = properties?.TryGetValue(Properties.LastUpdated, out var lu) == true ? lu?.ToString() : null;
        var isCurrent = properties?.TryGetValue(Properties.IsCurrent, out var ic) == true &&
                        (ic?.ToString()?.Equals(BooleanStrings.True, StringComparison.OrdinalIgnoreCase) == true || ic is true);
        var isDeleted = properties?.TryGetValue(Properties.IsDeleted, out var del) == true &&
                        (del?.ToString()?.Equals(BooleanStrings.True, StringComparison.OrdinalIgnoreCase) == true || del is true);

        DateTime? lastUpdated = null;
        if (DateTime.TryParse(lastUpdatedStr, out var parsed))
            lastUpdated = parsed;

        return new VersionInfo(versionId, lastUpdated, isCurrent, isDeleted);
    }

    /// <summary>
    /// Converts a graph vertex to a history entry.
    /// </summary>
    public static HistoryEntry ToHistoryEntry(
        string resourceType,
        string graphId,
        IDictionary<string, object>? properties)
    {
        var fhirId = properties?.TryGetValue(Properties.Id, out var id) == true ? id?.ToString() : null;
        var json = properties?.TryGetValue(Properties.Json, out var j) == true ? j?.ToString() : null;
        var versionInfo = ExtractVersionInfo(properties);

        return new HistoryEntry(
            graphId,
            fhirId,
            resourceType,
            json,
            versionInfo.VersionId,
            versionInfo.LastUpdated,
            versionInfo.IsDeleted);
    }
}
