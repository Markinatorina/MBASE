using static BLL.Constants;

namespace BLL.Models;

/// <summary>
/// Represents a parsed FHIR reference within a resource.
/// </summary>
public sealed record FhirReference(string Path, string TargetResourceType, string TargetFhirId)
{
    /// <summary>
    /// Gets the edge label for this reference in the format "fhir:ref:{path}".
    /// </summary>
    public string EdgeLabel => $"{EdgeLabels.FhirReferencePrefix}{Path}";
}
