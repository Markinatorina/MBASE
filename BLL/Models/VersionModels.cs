namespace BLL.Models;

/// <summary>
/// Contains version metadata for a FHIR resource.
/// </summary>
public sealed record VersionInfo(
    string? VersionId,
    DateTime? LastUpdated,
    bool IsCurrent,
    bool IsDeleted);

/// <summary>
/// Represents an entry in the version history of a FHIR resource.
/// </summary>
public sealed record HistoryEntry(
    string GraphId,
    string? FhirId,
    string? ResourceType,
    string? Json,
    string? VersionId,
    DateTime? LastUpdated,
    bool IsDeleted);
