using System.Text.Json;
using BLL.Models;
using static BLL.Constants;

namespace BLL.Utils;

/// <summary>
/// Provides helper methods for parsing and enumerating FHIR references.
/// </summary>
public static class FhirReferenceHelper
{
    /// <summary>
    /// Enumerates all relative references within a FHIR resource.
    /// </summary>
    /// <param name="root">The root JSON element of the FHIR resource.</param>
    /// <returns>An enumerable of FHIR references found in the resource.</returns>
    public static IEnumerable<FhirReference> EnumerateRelativeReferences(JsonElement root)
    {
        return EnumerateRelativeReferences(root, path: string.Empty);
    }

    private static IEnumerable<FhirReference> EnumerateRelativeReferences(JsonElement el, string path)
    {
        switch (el.ValueKind)
        {
            case JsonValueKind.Object:
                {
                    // "Reference" objects in FHIR commonly contain a string property named "reference".
                    if (el.TryGetProperty(Properties.Reference, out var refProp) && refProp.ValueKind == JsonValueKind.String)
                    {
                        var raw = refProp.GetString();
                        if (TryParseRelativeReference(raw, out var rt, out var id))
                        {
                            var p = string.IsNullOrEmpty(path) ? Properties.Reference : $"{path}.{Properties.Reference}";
                            yield return new FhirReference(p, rt, id);
                        }
                    }

                    foreach (var prop in el.EnumerateObject())
                    {
                        var childPath = string.IsNullOrEmpty(path)
                            ? prop.Name
                            : $"{path}.{prop.Name}";
                        foreach (var r in EnumerateRelativeReferences(prop.Value, childPath))
                            yield return r;
                    }
                    break;
                }
            case JsonValueKind.Array:
                {
                    var i = 0;
                    foreach (var item in el.EnumerateArray())
                    {
                        var childPath = $"{path}[{i}]";
                        foreach (var r in EnumerateRelativeReferences(item, childPath))
                            yield return r;
                        i++;
                    }
                    break;
                }
        }
    }

    /// <summary>
    /// Tries to parse a relative FHIR reference string (e.g., "Patient/123").
    /// </summary>
    /// <param name="raw">The raw reference string.</param>
    /// <param name="resourceType">The parsed resource type.</param>
    /// <param name="id">The parsed resource id.</param>
    /// <returns>True if the reference was successfully parsed as a relative reference.</returns>
    public static bool TryParseRelativeReference(string? raw, out string resourceType, out string id)
    {
        resourceType = string.Empty;
        id = string.Empty;

        if (string.IsNullOrWhiteSpace(raw))
            return false;

        // Relative references only, e.g. "Patient/123".
        // Reject absolute URLs, fragments, or other forms.
        if (raw.Contains("://", StringComparison.Ordinal) || raw.StartsWith("#", StringComparison.Ordinal))
            return false;

        var parts = raw.Split('/', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        if (parts.Length != 2)
            return false;

        resourceType = parts[0];
        id = parts[1];

        return !string.IsNullOrWhiteSpace(resourceType) && !string.IsNullOrWhiteSpace(id);
    }
}
