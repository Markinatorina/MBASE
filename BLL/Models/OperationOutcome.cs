namespace BLL.Models;

/// <summary>
/// Represents a FHIR OperationOutcome resource for API responses.
/// </summary>
public sealed class OperationOutcome
{
    public string ResourceType => "OperationOutcome";
    public IReadOnlyList<OperationOutcomeIssue> Issue { get; }

    public OperationOutcome(string severity, string code, string? diagnostics)
    {
        Issue = [new OperationOutcomeIssue(severity, code, diagnostics)];
    }

    public OperationOutcome(IReadOnlyList<OperationOutcomeIssue> issues)
    {
        Issue = issues;
    }

    /// <summary>
    /// Creates an OperationOutcome for an error response.
    /// </summary>
    public static OperationOutcome Error(string code, string? diagnostics)
        => new("error", code, diagnostics);

    /// <summary>
    /// Creates an OperationOutcome for a fatal error response.
    /// </summary>
    public static OperationOutcome Fatal(string code, string? diagnostics)
        => new("fatal", code, diagnostics);

    /// <summary>
    /// Creates an OperationOutcome for an informational response.
    /// </summary>
    public static OperationOutcome Information(string code, string? diagnostics)
        => new("information", code, diagnostics);

    /// <summary>
    /// Creates an OperationOutcome for a warning response.
    /// </summary>
    public static OperationOutcome Warning(string code, string? diagnostics)
        => new("warning", code, diagnostics);
}

/// <summary>
/// Represents an issue within a FHIR OperationOutcome.
/// </summary>
public sealed record OperationOutcomeIssue(
    string Severity,
    string Code,
    string? Diagnostics);
