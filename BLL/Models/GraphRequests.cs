namespace BLL.Models;

/// <summary>
/// Request model for creating an edge between two vertices.
/// </summary>
public sealed class CreateEdgeRequest
{
    public string Label { get; set; } = string.Empty;
    public string OutLabel { get; set; } = string.Empty;
    public string OutKey { get; set; } = string.Empty;
    public object OutValue { get; set; } = string.Empty;
    public string InLabel { get; set; } = string.Empty;
    public string InKey { get; set; } = string.Empty;
    public object InValue { get; set; } = string.Empty;
    public Dictionary<string, object>? Properties { get; set; }
}
