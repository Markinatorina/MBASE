namespace BLL.Models;

/// <summary>
/// Represents a response entry for a FHIR Bundle operation.
/// </summary>
public sealed record BundleResponseEntry(
    string Status,
    string? Location,
    string? Etag,
    string? Outcome,
    string? Resource = null)
{
    /// <summary>
    /// Serializes the entry to a JSON string for inclusion in a Bundle response.
    /// </summary>
    public string ToJson()
    {
        var parts = new List<string> { $"\"status\": \"{Status}\"" };
        if (Location != null) parts.Add($"\"location\": \"{Location}\"");
        if (Etag != null) parts.Add($"\"etag\": \"{Etag}\"");
        
        var response = "\"response\": {" + string.Join(", ", parts) + "}";
        
        if (Outcome != null)
        {
            var outcomeResource = $$"""
            "resource": {
                "resourceType": "OperationOutcome",
                "issue": [{"severity": "error", "code": "processing", "diagnostics": "{{Outcome.Replace("\"", "\\\"")}}"}]
            }
            """;
            return "{" + response + ", " + outcomeResource + "}";
        }
        
        if (Resource != null)
        {
            return "{" + response + ", \"resource\": " + Resource + "}";
        }
        
        return "{" + response + "}";
    }
}
