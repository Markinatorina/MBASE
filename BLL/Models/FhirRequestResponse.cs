using System.Text.Json;

namespace BLL.Models;

#region FHIR Request Models

public sealed record CreateResourceRequest(
    JsonDocument Json,
    string ResourceType,
    bool MaterializeReferences = false,
    bool AllowPlaceholderTargets = false,
    string? IfNoneExist = null);

public sealed record UpdateResourceRequest(
    JsonDocument Json,
    string ResourceType,
    string FhirId,
    bool MaterializeReferences = false,
    bool AllowPlaceholderTargets = false,
    string? IfMatch = null);

public sealed record PatchResourceRequest(
    JsonDocument PatchDocument,
    string ResourceType,
    string FhirId,
    string? IfMatch = null);

public sealed record DeleteResourceRequest(
    string ResourceType,
    string FhirId,
    string? IfMatch = null);

public sealed record ConditionalRequest(
    string ResourceType,
    IDictionary<string, object> SearchCriteria);

public sealed record SearchRequest(
    string? ResourceType = null,
    IReadOnlyList<string>? ResourceTypes = null,
    IDictionary<string, object>? Filters = null,
    int Limit = 100,
    int Offset = 0);

public sealed record HistoryRequest(
    string? ResourceType = null,
    string? FhirId = null,
    int Limit = 100,
    DateTime? Since = null);

public sealed record BundleRequest(
    JsonDocument BundleDocument);

#endregion

#region Common Operation Result

/// <summary>
/// Standard operation result for all API operations.
/// </summary>
public sealed record OperationResult(
    bool Success,
    int StatusCode,
    object? Body = null)
{
    public static OperationResult Ok(object? body = null) 
        => new(true, 200, body);
    
    public static OperationResult Created(object body) 
        => new(true, 201, body);
    
    public static OperationResult NoContent() 
        => new(true, 204);
    
    public static OperationResult BadRequest(object body) 
        => new(false, 400, body);
    
    public static OperationResult NotFound(object body) 
        => new(false, 404, body);
    
    public static OperationResult InternalError(object body) 
        => new(false, 500, body);
}

#endregion

#region FHIR Response Models

public sealed record FhirOperationResult(
    bool Success,
    int StatusCode,
    object? Body = null,
    string? Location = null,
    string? ETag = null,
    string? LastModified = null)
{
    public static FhirOperationResult Ok(object? body = null) 
        => new(true, 200, body);
    
    public static FhirOperationResult Created(object? body, string? location = null, string? etag = null) 
        => new(true, 201, body, location, etag);
    
    public static FhirOperationResult NoContent() 
        => new(true, 204);
    
    public static FhirOperationResult NotModified() 
        => new(true, 304);
    
    public static FhirOperationResult BadRequest(object body) 
        => new(false, 400, body);
    
    public static FhirOperationResult NotFound(object body) 
        => new(false, 404, body);
    
    public static FhirOperationResult Gone(object body) 
        => new(false, 410, body);
    
    public static FhirOperationResult PreconditionFailed(object body) 
        => new(false, 412, body);
    
    public static FhirOperationResult UnprocessableEntity(object body) 
        => new(false, 422, body);
    
    public static FhirOperationResult InternalError(object body) 
        => new(false, 500, body);
}

public sealed record CreateResourceResult(
    bool Success,
    int StatusCode,
    object? Body,
    string? GraphId = null,
    string? FhirId = null,
    bool Created = false,
    string? Location = null,
    string? ETag = null);

public sealed record ReadResourceResult(
    bool Success,
    int StatusCode,
    string? Json = null,
    object? Body = null,
    string? ETag = null,
    string? LastModified = null);

public sealed record SearchResourceResult(
    bool Success,
    int StatusCode,
    object? Bundle = null,
    object? Body = null);

#endregion
