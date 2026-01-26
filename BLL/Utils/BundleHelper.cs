using System.Text.Json;
using BLL.Models;
using static BLL.Constants;

namespace BLL.Utils;

/// <summary>
/// Provides helper methods for working with FHIR Bundles.
/// </summary>
public static class BundleHelper
{
    /// <summary>
    /// Creates a response Bundle from a list of entry responses.
    /// </summary>
    /// <param name="bundleType">The type of Bundle (e.g., "batch-response", "transaction-response").</param>
    /// <param name="entries">The list of response entries.</param>
    /// <returns>A JSON document containing the response Bundle.</returns>
    public static JsonDocument CreateResponseBundle(string bundleType, IReadOnlyList<BundleResponseEntry> entries)
    {
        var json = $$"""
        {
            "resourceType": "{{ResourceTypes.Bundle}}",
            "type": "{{bundleType}}",
            "entry": [{{string.Join(",", entries.Select(e => e.ToJson()))}}]
        }
        """;
        return JsonDocument.Parse(json);
    }
}
