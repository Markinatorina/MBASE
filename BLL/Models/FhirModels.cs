namespace BLL.Models;

/// <summary>
/// Represents a FHIR resource search result.
/// </summary>
public sealed record FhirSearchResult(
    string GraphId,
    string? FhirId,
    string? ResourceType,
    string? Json,
    bool IsPlaceholder);

/// <summary>
/// Represents an outgoing FHIR reference from a resource.
/// </summary>
public sealed record FhirReferenceResult(
    string? Path,
    string? TargetResourceType,
    string? TargetFhirId,
    string TargetGraphId);

/// <summary>
/// Represents an incoming FHIR reference to a resource.
/// </summary>
public sealed record FhirReferrerResult(
    string? SourceResourceType,
    string? SourceFhirId,
    string SourceGraphId,
    string? ReferencePath);

/// <summary>
/// Represents the result of a Patient/$everything operation.
/// </summary>
public sealed record PatientEverythingResult(
    string PatientId,
    IReadOnlyList<FhirSearchResult> Resources);
