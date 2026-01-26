using System.Text.Json;
using BLL.Models;
using BLL.Utils;
using Microsoft.Extensions.Logging;
using static BLL.Constants;

namespace BLL.Services;

/// <summary>
/// Service for FHIR controller-ready API operations.
/// Orchestrates focused services to provide complete FHIR REST API functionality.
/// 
/// This service implements the FHIR RESTful API as defined in:
/// https://build.fhir.org/http.html (FHIR R6 6.0.0-ballot3)
/// 
/// Key specifications implemented:
/// - §3.2.0.2  read: GET [base]/[type]/[id] - Read current state of resource
/// - §3.2.0.3  vread: GET [base]/[type]/[id]/_history/[vid] - Read specific version
/// - §3.2.0.4  update: PUT [base]/[type]/[id] - Update existing resource
/// - §3.2.0.6  patch: PATCH [base]/[type]/[id] - Partial update with JSON Patch
/// - §3.2.0.7  delete: DELETE [base]/[type]/[id] - Remove resource
/// - §3.2.0.8  delete-history: DELETE [base]/[type]/[id]/_history - Remove all history
/// - §3.2.0.9  delete-history-version: DELETE [base]/[type]/[id]/_history/[vid] - Remove specific version
/// - §3.2.0.10 create: POST [base]/[type] - Create new resource
/// - §3.2.0.11 search: GET/POST [base]/[type]? - Search resources
/// - §3.2.0.12 capabilities: GET [base]/metadata - Server capability statement
/// - §3.2.0.13 batch/transaction: POST [base] - Multiple operations in single request
/// - §3.2.0.14 history: GET [base]/[type]/[id]/_history - Retrieve change history
/// 
/// HTTP Headers (§3.2.0.1.6):
/// - ETag: W/"[versionId]" (weak ETag per §3.2.0.1.3)
/// - If-Match: Version-aware updates (§3.2.0.5)
/// - If-None-Match: Conditional read (§3.2.0.1.8)
/// - If-None-Exist: Conditional create (§3.2.0.10.1)
/// - Location: [base]/[type]/[id]/_history/[vid] for create/update
/// - Last-Modified: From meta.lastUpdated
/// </summary>
public class FHIRService
{
    private readonly ILogger<FHIRService> _logger;
    private readonly FhirValidationService _validation;
    private readonly FhirPersistenceService _persistence;
    private readonly FhirConditionalService _conditional;
    private readonly FhirVersioningService _versioning;
    private readonly FhirBundleService _bundle;
    private readonly FhirPatientService _patient;

    public FHIRService(
        ILogger<FHIRService> logger,
        FhirValidationService validation,
        FhirPersistenceService persistence,
        FhirConditionalService conditional,
        FhirVersioningService versioning,
        FhirBundleService bundle,
        FhirPatientService patient)
    {
        _logger = logger;
        _validation = validation;
        _persistence = persistence;
        _conditional = conditional;
        _versioning = versioning;
        _bundle = bundle;
        _patient = patient;
    }

    /// <summary>
    /// Creates a FHIR resource with full request handling.
    /// 
    /// FHIR Spec Reference: §3.2.0.10 create (https://build.fhir.org/http.html#create)
    /// 
    /// Request: POST [base]/[type] with Resource body
    /// 
    /// Success Response:
    /// - 201 Created: Resource was created
    ///   - Location header: [base]/[type]/[id]/_history/[vid] (REQUIRED per spec)
    ///   - ETag: W/"[versionId]" (SHOULD per spec)
    /// - 200 OK: Conditional create found existing match (single match case)
    /// 
    /// Error Response:
    /// - 400 Bad Request: Resource could not be parsed or failed FHIR validation
    /// - 412 Precondition Failed: Conditional create found multiple matches
    /// - 422 Unprocessable Entity: Violated FHIR profiles or business rules
    /// 
    /// Conditional Create (§3.2.0.10.1):
    /// When If-None-Exist header is provided:
    /// - No matches: Create resource normally, return 201
    /// - One match: Return 200 OK (resource already exists)
    /// - Multiple matches: Return 412 Precondition Failed
    /// 
    /// Per spec: "If the request body includes a meta, the server SHALL ignore the 
    /// provided versionId and lastUpdated values."
    /// </summary>
    public async Task<FhirOperationResult> CreateResourceAsync(
        CreateResourceRequest request,
        string baseUrl,
        CancellationToken ct = default)
    {
        try
        {
            if (request.Json.RootElement.TryGetProperty(Properties.ResourceType, out var rtProp) &&
                rtProp.GetString() != request.ResourceType)
            {
                return FhirOperationResult.BadRequest(
                    FhirResponseHelper.CreateOperationOutcome(Severity.Error, IssueCodes.Invalid,
                        $"Resource type mismatch: URL specifies {request.ResourceType} but payload has {rtProp.GetString()}"));
            }

            string? graphId, fhirId;
            int? materializedReferenceCount;
            bool created;

            if (!string.IsNullOrEmpty(request.IfNoneExist))
            {
                var searchCriteria = FhirResponseHelper.ParseSearchParameters(request.IfNoneExist);
                var result = await _conditional.ConditionalCreateAsync(
                    request.Json, request.ResourceType, searchCriteria,
                    request.MaterializeReferences, request.AllowPlaceholderTargets, ct);

                if (!result.ok)
                {
                    return result.error?.Contains("412") == true
                        ? FhirOperationResult.PreconditionFailed(
                            FhirResponseHelper.CreateOperationOutcome(Severity.Error, IssueCodes.Duplicate, result.error))
                        : FhirOperationResult.BadRequest(
                            FhirResponseHelper.CreateOperationOutcome(Severity.Error, IssueCodes.Invalid, result.error));
                }

                graphId = result.graphId;
                fhirId = result.fhirId;
                materializedReferenceCount = result.materializedCount;
                created = result.created;
            }
            else
            {
                var result = await _persistence.ValidateAndPersistAsync(
                    request.Json, request.MaterializeReferences, request.AllowPlaceholderTargets, ct);

                if (!result.ok)
                    return FhirOperationResult.BadRequest(
                        FhirResponseHelper.CreateOperationOutcome(Severity.Error, IssueCodes.Invalid, result.error));

                graphId = result.graphId;
                fhirId = result.fhirId;
                materializedReferenceCount = result.materializedReferenceCount;
                created = true;
            }

            var location = $"{baseUrl}/{request.ResourceType}/{fhirId}";
            var body = request.MaterializeReferences
                ? new { graphId, fhirId, created, materializedReferenceCount = materializedReferenceCount ?? 0 }
                : (object)new { graphId, fhirId, created };

            return created
                ? FhirOperationResult.Created(body, location, $"W/\"{graphId}\"")
                : FhirOperationResult.Ok(body);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Create resource failed for {ResourceType}", request.ResourceType);
            return FhirOperationResult.InternalError(
                FhirResponseHelper.CreateOperationOutcome(Severity.Fatal, IssueCodes.Exception, ex.Message));
        }
    }

    /// <summary>
    /// Reads a FHIR resource with conditional handling.
    /// 
    /// FHIR Spec Reference: §3.2.0.2 read (https://build.fhir.org/http.html#read)
    /// 
    /// Request: GET [base]/[type]/[id]
    /// 
    /// Success Response:
    /// - 200 OK: Resource found and returned
    ///   - ETag: W/"[versionId]" (SHOULD per spec)
    ///   - Body: Resource with id element matching [id]
    /// - 304 Not Modified: Conditional read with matching If-None-Match (§3.2.0.1.8)
    /// 
    /// Error Response:
    /// - 404 Not Found: Unknown resource
    /// - 410 Gone: Deleted resource (per spec: "GET for a deleted resource returns 410 Gone")
    /// 
    /// Conditional Read (§3.2.0.1.8):
    /// "Clients may use the If-Modified-Since, or If-None-Match HTTP header on a read request.
    /// If so, they SHALL accept either a 304 Not Modified as a valid status code on the response."
    /// 
    /// Per spec: "Servers SHOULD return an ETag header with the versionId of the resource 
    /// (if versioning is supported) and a Last-Modified header."
    /// </summary>
    public async Task<FhirOperationResult> ReadResourceAsync(
        string resourceType,
        string fhirId,
        string? ifNoneMatch = null,
        CancellationToken ct = default)
    {
        var (ok, error, json, graphId) = await _persistence.GetByResourceTypeAndIdAsync(resourceType, fhirId, ct);

        if (!ok)
            return FhirOperationResult.NotFound(
                FhirResponseHelper.CreateOperationOutcome(Severity.Error, IssueCodes.NotFound, error));

        var etag = graphId != null ? $"W/\"{graphId}\"" : null;

        if (!string.IsNullOrEmpty(ifNoneMatch) && ifNoneMatch == etag)
            return FhirOperationResult.NotModified();

        return new FhirOperationResult(true, 200, json, ETag: etag);
    }

    /// <summary>
    /// Updates a FHIR resource with version checking.
    /// 
    /// FHIR Spec Reference: §3.2.0.4 update (https://build.fhir.org/http.html#update)
    /// 
    /// Request: PUT [base]/[type]/[id] with Resource body
    /// 
    /// Success Response:
    /// - 200 OK: Resource was updated
    ///   - Location header: [base]/[type]/[id]/_history/[vid] (SHALL per spec)
    ///   - ETag: W/"[versionId]" (SHOULD per spec)
    /// - 201 Created: Resource was created (upsert case per §3.2.0.4.1)
    /// 
    /// Error Response:
    /// - 400 Bad Request: "If no id element is provided, or the id disagrees with the id in the URL,
    ///   the server SHALL respond with HTTP 400 Bad Request"
    /// - 404 Not Found: Resource type not supported
    /// - 409 Conflict/412 Precondition Failed: Version conflict (§3.2.0.5)
    /// - 422 Unprocessable Entity: Violated FHIR profiles or business rules
    /// 
    /// Version-Aware Updates (§3.2.0.5):
    /// "If the version id given in the If-Match header does not match, the server returns 
    /// a 412 Precondition Failed status code instead of updating the resource."
    /// 
    /// Per spec: "If the request body includes a meta, the server SHALL ignore the provided 
    /// versionId and lastUpdated values."
    /// </summary>
    public async Task<FhirOperationResult> UpdateResourceAsync(
        UpdateResourceRequest request,
        string baseUrl,
        CancellationToken ct = default)
    {
        try
        {
            if (request.Json.RootElement.TryGetProperty(Properties.ResourceType, out var rtProp) &&
                rtProp.GetString() != request.ResourceType)
            {
                return FhirOperationResult.BadRequest(
                    FhirResponseHelper.CreateOperationOutcome(Severity.Error, IssueCodes.Invalid,
                        $"Resource type mismatch: URL specifies {request.ResourceType} but payload has {rtProp.GetString()}"));
            }

            if (request.Json.RootElement.TryGetProperty(Properties.Id, out var idProp) &&
                idProp.GetString() != request.FhirId)
            {
                return FhirOperationResult.BadRequest(
                    FhirResponseHelper.CreateOperationOutcome(Severity.Error, IssueCodes.Invalid,
                        $"ID mismatch: URL specifies {request.FhirId} but payload has {idProp.GetString()}"));
            }

            if (!string.IsNullOrEmpty(request.IfMatch))
            {
                var (existsOk, _, _, existingGraphId) = await _persistence.GetByResourceTypeAndIdAsync(
                    request.ResourceType, request.FhirId, ct);
                if (existsOk && existingGraphId != null)
                {
                    var expectedEtag = $"W/\"{existingGraphId}\"";
                    if (request.IfMatch != expectedEtag)
                    {
                        return FhirOperationResult.PreconditionFailed(
                            FhirResponseHelper.CreateOperationOutcome(Severity.Error, IssueCodes.Conflict,
                                "Version mismatch - resource has been modified"));
                    }
                }
            }

            var (ok, error, graphId, fhirId, materializedCount) = await _persistence.ValidateAndPersistAsync(
                request.Json, request.MaterializeReferences, request.AllowPlaceholderTargets, ct);

            if (!ok)
                return FhirOperationResult.BadRequest(
                    FhirResponseHelper.CreateOperationOutcome(Severity.Error, IssueCodes.Invalid, error));

            var location = $"{baseUrl}/{request.ResourceType}/{fhirId}";
            var etag = $"W/\"{graphId}\"";

            return new FhirOperationResult(true, 200,
                new { graphId, fhirId, materializedReferenceCount = materializedCount ?? 0 },
                location, etag);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Update resource failed for {ResourceType}/{FhirId}", request.ResourceType, request.FhirId);
            return FhirOperationResult.InternalError(
                FhirResponseHelper.CreateOperationOutcome(Severity.Fatal, IssueCodes.Exception, ex.Message));
        }
    }

    /// <summary>
    /// Patches a FHIR resource with version checking.
    /// 
    /// FHIR Spec Reference: §3.2.0.6 patch (https://build.fhir.org/http.html#patch)
    /// 
    /// Request: PATCH [base]/[type]/[id] with patch document
    /// Supported Content-Types:
    /// - application/json-patch+json (JSON Patch per RFC 6902)
    /// - application/fhir+json (FHIRPath Patch)
    /// 
    /// Success Response:
    /// - 200 OK: Resource was patched
    ///   - ETag: W/"[versionId]" (SHOULD per spec)
    /// 
    /// Error Response:
    /// - 404 Not Found: Resource does not exist
    /// - 412 Precondition Failed: Version mismatch via If-Match
    /// - 422 Unprocessable Entity: Patch operations failed (e.g., JSON Patch test failure)
    /// 
    /// Per spec: "Processing PATCH operations may be very version sensitive. For this reason, 
    /// servers that support PATCH SHALL support Resource Contention on the PATCH interaction.
    /// Clients SHOULD always consider using version specific PATCH interactions."
    /// 
    /// Per spec: "In the case of a failing JSON Patch test interaction, the server returns 
    /// a 422 Unprocessable Entity."
    /// </summary>
    public async Task<FhirOperationResult> PatchResourceAsync(
        PatchResourceRequest request,
        CancellationToken ct = default)
    {
        try
        {
            if (!string.IsNullOrEmpty(request.IfMatch))
            {
                var (existsOk, _, _, existingGraphId) = await _persistence.GetByResourceTypeAndIdAsync(
                    request.ResourceType, request.FhirId, ct);
                if (existsOk && existingGraphId != null)
                {
                    var expectedEtag = $"W/\"{existingGraphId}\"";
                    if (request.IfMatch != expectedEtag)
                    {
                        return FhirOperationResult.PreconditionFailed(
                            FhirResponseHelper.CreateOperationOutcome(Severity.Error, IssueCodes.Conflict,
                                "Version mismatch - resource has been modified"));
                    }
                }
            }

            var (ok, error, graphId, fhirId) = await _conditional.PatchAsync(
                request.ResourceType, request.FhirId, request.PatchDocument, ct);

            if (!ok)
            {
                if (error?.Contains("not found") == true)
                    return FhirOperationResult.NotFound(
                        FhirResponseHelper.CreateOperationOutcome(Severity.Error, IssueCodes.NotFound, error));
                return FhirOperationResult.UnprocessableEntity(
                    FhirResponseHelper.CreateOperationOutcome(Severity.Error, IssueCodes.Invalid, error));
            }

            return new FhirOperationResult(true, 200, new { graphId, fhirId }, ETag: $"W/\"{graphId}\"");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Patch resource failed for {ResourceType}/{FhirId}", request.ResourceType, request.FhirId);
            return FhirOperationResult.InternalError(
                FhirResponseHelper.CreateOperationOutcome(Severity.Fatal, IssueCodes.Exception, ex.Message));
        }
    }

    /// <summary>
    /// Deletes a FHIR resource with version checking.
    /// 
    /// FHIR Spec Reference: §3.2.0.7 delete (https://build.fhir.org/http.html#delete)
    /// 
    /// Request: DELETE [base]/[type]/[id]
    /// 
    /// Success Response:
    /// - 200 OK: If the response contains a payload
    /// - 204 No Content: With no response payload (our implementation)
    /// 
    /// Error Response:
    /// - 404 Not Found: Resource does not exist (per spec "performing this interaction on a 
    ///   resource that does not exist has no effect")
    /// - 405 Method Not Allowed: Server refuses to delete resources of this type as policy
    /// - 409 Conflict: Server-side pessimistic locking or business rule conflict
    /// - 412 Precondition Failed: If-Match version mismatch
    /// 
    /// Per spec: "Upon successful deletion, or if the resource does not exist at all, 
    /// the server should return either a 200 OK if the response contains a payload, 
    /// or a 204 No Content with no response payload."
    /// 
    /// Per spec: "A delete interaction means that subsequent non-version specific reads of the 
    /// resource return a 410 Gone HTTP status code."
    /// 
    /// Per spec: "Resources that have been deleted may be 'brought back to life' by a 
    /// subsequent update interaction using an HTTP PUT."
    /// </summary>
    public async Task<FhirOperationResult> DeleteResourceAsync(
        DeleteResourceRequest request,
        CancellationToken ct = default)
    {
        if (!string.IsNullOrEmpty(request.IfMatch))
        {
            var (existsOk, _, _, existingGraphId) = await _persistence.GetByResourceTypeAndIdAsync(
                request.ResourceType, request.FhirId, ct);
            if (existsOk && existingGraphId != null)
            {
                var expectedEtag = $"W/\"{existingGraphId}\"";
                if (request.IfMatch != expectedEtag)
                {
                    return FhirOperationResult.PreconditionFailed(
                        FhirResponseHelper.CreateOperationOutcome(Severity.Error, IssueCodes.Conflict,
                            "Version mismatch - resource has been modified"));
                }
            }
        }

        var (ok, error) = await _persistence.DeleteByResourceTypeAndIdAsync(request.ResourceType, request.FhirId, ct);

        return ok
            ? FhirOperationResult.NoContent()
            : FhirOperationResult.NotFound(
                FhirResponseHelper.CreateOperationOutcome(Severity.Error, IssueCodes.NotFound, error));
    }

    /// <summary>
    /// Reads a specific version of a resource.
    /// 
    /// FHIR Spec Reference: §3.2.0.3 vread (https://build.fhir.org/http.html#vread)
    /// 
    /// Request: GET [base]/[type]/[id]/_history/[vid]
    /// 
    /// Success Response:
    /// - 200 OK: Version found and returned
    ///   - ETag: W/"[versionId]" (SHOULD per spec)
    ///   - Last-Modified header (SHOULD per spec)
    ///   - Body: Resource with id=[id] and meta.versionId=[vid]
    /// 
    /// Error Response:
    /// - 404 Not Found: Version does not exist, OR server does not support version history
    /// - 410 Gone: "If the version referred to is actually one where the resource was deleted,
    ///   the server should return a 410 Gone status code"
    /// 
    /// Per spec: "The returned resource SHALL have an id element with a value that is the [id], 
    /// and a meta.versionId element with a value of [vid]."
    /// 
    /// Per spec: "If a request is made for a previous version of a resource, and the server 
    /// does not support accessing previous versions, it should return a 404 Not Found error."
    /// </summary>
    public async Task<FhirOperationResult> ReadVersionAsync(
        string resourceType,
        string fhirId,
        string versionId,
        CancellationToken ct = default)
    {
        var (ok, error, json, graphId, versionInfo) = await _versioning.GetVersionAsync(
            resourceType, fhirId, versionId, ct);

        if (!ok)
        {
            if (versionInfo?.IsDeleted == true)
                return FhirOperationResult.Gone(
                    FhirResponseHelper.CreateOperationOutcome(Severity.Error, IssueCodes.Deleted,
                        $"{resourceType}/{fhirId} was deleted at version {versionId}"));

            return FhirOperationResult.NotFound(
                FhirResponseHelper.CreateOperationOutcome(Severity.Error, IssueCodes.NotFound, error));
        }

        return new FhirOperationResult(true, 200, json,
            ETag: $"W/\"{versionId}\"",
            LastModified: versionInfo?.LastUpdated?.ToString("R"));
    }

    /// <summary>
    /// Conditional delete with search criteria.
    /// 
    /// FHIR Spec Reference: §3.2.0.7.1 Conditional delete (https://build.fhir.org/http.html#cdelete)
    /// 
    /// Request: DELETE [base]/[type]?[search parameters]
    /// 
    /// Behavior based on match count:
    /// - No matches: Return 404 Not Found (per spec "attempts an ordinary delete")
    /// - One Match: Perform ordinary delete, return 200/204
    /// - Multiple matches: 
    ///   - delete-conditional-single: Return 412 Precondition Failed
    ///   - delete-conditional-multiple: Delete all matching resources
    /// 
    /// Our implementation uses delete-conditional-single (allowMultiple: false) by default
    /// to prevent accidental mass deletion.
    /// 
    /// Per spec: "A server may choose to delete all the matching resources (delete-conditional-multiple), 
    /// or it may choose to return a 412 Precondition Failed error indicating the client's criteria 
    /// were not selective enough (delete-conditional-single)."
    /// </summary>
    public async Task<FhirOperationResult> ConditionalDeleteResourceAsync(
        ConditionalRequest request,
        CancellationToken ct = default)
    {
        if (request.SearchCriteria.Count == 0)
        {
            return FhirOperationResult.BadRequest(
                FhirResponseHelper.CreateOperationOutcome(Severity.Error, IssueCodes.Invalid,
                    "Conditional delete requires search parameters"));
        }

        var (ok, error, deletedCount) = await _conditional.ConditionalDeleteAsync(
            request.ResourceType, request.SearchCriteria, allowMultiple: false, ct);

        if (!ok)
        {
            return error?.Contains("412") == true
                ? FhirOperationResult.PreconditionFailed(
                    FhirResponseHelper.CreateOperationOutcome(Severity.Error, IssueCodes.MultipleMatches, error))
                : FhirOperationResult.BadRequest(
                    FhirResponseHelper.CreateOperationOutcome(Severity.Error, IssueCodes.Invalid, error));
        }

        return deletedCount > 0
            ? FhirOperationResult.NoContent()
            : FhirOperationResult.NotFound(
                FhirResponseHelper.CreateOperationOutcome(Severity.Warning, IssueCodes.NotFound, "No matching resources found"));
    }

    /// <summary>
    /// Conditional update with search criteria.
    /// 
    /// FHIR Spec Reference: §3.2.0.4.3 Conditional update (https://build.fhir.org/http.html#cond-update)
    /// 
    /// Request: PUT [base]/[type]?[search parameters]
    /// 
    /// Behavior based on match count:
    /// - No matches, no id provided: Server creates the resource
    /// - No matches, id provided and doesn't exist: Treat as Update as Create (§3.2.0.4.1)
    /// - No matches, id provided and exists: Reject with 409 Conflict
    /// - One Match, resource id matches or not provided: Update the matching resource
    /// - One Match, resource id differs from found resource: Return 400 Bad Request
    /// - Multiple matches: Return 412 Precondition Failed
    /// 
    /// Success Response:
    /// - 200 OK: Resource was updated
    /// - 201 Created: Resource was created
    /// Both include Location header with [base]/[type]/[id]/_history/[vid]
    /// 
    /// Per spec: "This variant can be used to allow a stateless client to submit updated 
    /// results to a server, without having to remember the logical ids that the server 
    /// has assigned."
    /// </summary>
    public async Task<FhirOperationResult> ConditionalUpdateResourceAsync(
        JsonDocument json,
        ConditionalRequest request,
        bool materializeReferences,
        bool allowPlaceholderTargets,
        string baseUrl,
        CancellationToken ct = default)
    {
        if (request.SearchCriteria.Count == 0)
        {
            return FhirOperationResult.BadRequest(
                FhirResponseHelper.CreateOperationOutcome(Severity.Error, IssueCodes.Invalid,
                    "Conditional update requires search parameters"));
        }

        try
        {
            if (json.RootElement.TryGetProperty(Properties.ResourceType, out var rtProp) &&
                rtProp.GetString() != request.ResourceType)
            {
                return FhirOperationResult.BadRequest(
                    FhirResponseHelper.CreateOperationOutcome(Severity.Error, IssueCodes.Invalid,
                        $"Resource type mismatch: URL specifies {request.ResourceType} but payload has {rtProp.GetString()}"));
            }

            var (ok, error, graphId, fhirId, created, materializedCount) = await _conditional.ConditionalUpdateAsync(
                json, request.ResourceType, request.SearchCriteria, materializeReferences, allowPlaceholderTargets, ct);

            if (!ok)
            {
                return error?.Contains("412") == true
                    ? FhirOperationResult.PreconditionFailed(
                        FhirResponseHelper.CreateOperationOutcome(Severity.Error, IssueCodes.MultipleMatches, error))
                    : FhirOperationResult.BadRequest(
                        FhirResponseHelper.CreateOperationOutcome(Severity.Error, IssueCodes.Invalid, error));
            }

            var location = $"{baseUrl}/{request.ResourceType}/{fhirId}";
            var body = new { graphId, fhirId, created, materializedReferenceCount = materializedCount ?? 0 };

            return created
                ? FhirOperationResult.Created(body, location, $"W/\"{graphId}\"")
                : new FhirOperationResult(true, 200, body, location, $"W/\"{graphId}\"");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Conditional update failed for {ResourceType}", request.ResourceType);
            return FhirOperationResult.InternalError(
                FhirResponseHelper.CreateOperationOutcome(Severity.Fatal, IssueCodes.Exception, ex.Message));
        }
    }

    /// <summary>
    /// Conditional patch with search criteria.
    /// 
    /// FHIR Spec Reference: §3.2.0.6.1 Conditional patch (https://build.fhir.org/http.html#cond-patch)
    /// 
    /// Request: PATCH [base]/[type]?[search parameters]
    /// 
    /// Behavior based on match count:
    /// - No matches: Return 404 Not Found
    /// - One Match: Perform the patch on the matching resource
    /// - Multiple matches: Return 412 Precondition Failed
    /// 
    /// Per spec: "Servers that support PATCH, and that support Conditional Update SHOULD 
    /// also support conditional patch."
    /// </summary>
    public async Task<FhirOperationResult> ConditionalPatchResourceAsync(
        JsonDocument patchDocument,
        ConditionalRequest request,
        CancellationToken ct = default)
    {
        if (request.SearchCriteria.Count == 0)
        {
            return FhirOperationResult.BadRequest(
                FhirResponseHelper.CreateOperationOutcome(Severity.Error, IssueCodes.Invalid,
                    "Conditional patch requires search parameters"));
        }

        try
        {
            var (ok, error, graphId, fhirId) = await _conditional.ConditionalPatchAsync(
                request.ResourceType, request.SearchCriteria, patchDocument, ct);

            if (!ok)
            {
                if (error?.Contains("404") == true)
                    return FhirOperationResult.NotFound(
                        FhirResponseHelper.CreateOperationOutcome(Severity.Error, IssueCodes.NotFound, error));
                if (error?.Contains("412") == true)
                    return FhirOperationResult.PreconditionFailed(
                        FhirResponseHelper.CreateOperationOutcome(Severity.Error, IssueCodes.MultipleMatches, error));
                return FhirOperationResult.UnprocessableEntity(
                    FhirResponseHelper.CreateOperationOutcome(Severity.Error, IssueCodes.Invalid, error));
            }

            return new FhirOperationResult(true, 200, new { graphId, fhirId });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Conditional patch failed for {ResourceType}", request.ResourceType);
            return FhirOperationResult.InternalError(
                FhirResponseHelper.CreateOperationOutcome(Severity.Fatal, IssueCodes.Exception, ex.Message));
        }
    }

    /// <summary>
    /// Searches resources and returns a FHIR Bundle.
    /// 
    /// FHIR Spec Reference: §3.2.0.11 search (https://build.fhir.org/http.html#search)
    /// 
    /// Request variants:
    /// - Type-level: GET [base]/[type]?[parameters]
    /// - System-level: GET [base]?[parameters]
    /// - POST with form data: POST [base]/[type]/_search
    /// 
    /// Success Response:
    /// - 200 OK: Search completed
    ///   - Body: Bundle with type="searchset"
    ///   - Bundle.total: Total number of resources matching (MAY be provided per spec)
    ///   - Bundle.link: Self link with original query URL
    ///   - Bundle.entry[].fullUrl: Full URL of each resource
    ///   - Bundle.entry[].search.mode: "match" for matching resources
    /// 
    /// Error Response:
    /// - 400 Bad Request: Search could not be processed or failed validation
    /// - 401 Unauthorized: Authorization required
    /// - 404 Not Found: Resource type not supported
    /// 
    /// Per spec: "If the search succeeds, the server SHALL return a 200 OK HTTP status code 
    /// and the return content SHALL be a Bundle with type = searchset containing the results 
    /// of the search as a collection of zero or more resources in a defined order."
    /// 
    /// Per spec: "Servers SHALL support both POST and GET for search, though MAY return 
    /// HTTP 405 for either (but not both)."
    /// </summary>
    public async Task<FhirOperationResult> SearchResourcesAsync(
        SearchRequest request,
        string selfUrl,
        string baseUrl,
        CancellationToken ct = default)
    {
        IReadOnlyList<FhirSearchResult> results;
        long totalCount;

        if (request.ResourceType != null)
        {
            var (ok, error, r, c) = await _persistence.SearchAsync(
                request.ResourceType, request.Filters, request.Limit, request.Offset, ct);
            if (!ok)
                return FhirOperationResult.BadRequest(
                    FhirResponseHelper.CreateOperationOutcome(Severity.Error, IssueCodes.Invalid, error));
            results = r;
            totalCount = c;
        }
        else
        {
            var (ok, error, r, c) = await _persistence.SearchAllTypesAsync(
                request.ResourceTypes, request.Filters, request.Limit, request.Offset, ct);
            if (!ok)
                return FhirOperationResult.BadRequest(
                    FhirResponseHelper.CreateOperationOutcome(Severity.Error, IssueCodes.Invalid, error));
            results = r;
            totalCount = c;
        }

        var bundle = new
        {
            resourceType = ResourceTypes.Bundle,
            type = BundleTypes.SearchSet,
            total = totalCount,
            link = new[] { new { relation = Status.Self, url = selfUrl } },
            entry = results.Where(r => r.Json != null).Select(r => new
            {
                fullUrl = $"{baseUrl}/{r.ResourceType}/{r.FhirId}",
                resource = JsonSerializer.Deserialize<JsonElement>(r.Json!),
                search = new { mode = Status.Match }
            }).ToArray()
        };

        return FhirOperationResult.Ok(bundle);
    }

    /// <summary>
    /// Gets history and returns a FHIR Bundle.
    /// 
    /// FHIR Spec Reference: §3.2.0.14 history (https://build.fhir.org/http.html#history)
    /// 
    /// Request variants:
    /// - Instance: GET [base]/[type]/[id]/_history
    /// - Type: GET [base]/[type]/_history
    /// - System: GET [base]/_history
    /// 
    /// Success Response:
    /// - 200 OK: History retrieved
    ///   - Body: Bundle with type="history"
    ///   - Sorted with oldest versions last
    ///   - Each entry contains request (method/url) and response (status/lastModified)
    ///   - Deleted resources included as entries with no resource but with request.method=DELETE
    /// 
    /// Parameters supported:
    /// - _count: Maximum entries per page
    /// - _since: Only include versions created at or after the given instant
    /// 
    /// Per spec: "The return content is a Bundle with type set to history containing the 
    /// specified version history, sorted with oldest versions last, and including deleted resources."
    /// 
    /// Per spec: "Each entry SHALL minimally contain at least one of: a resource which holds 
    /// the resource as it is at the conclusion of the interaction, or a request with 
    /// entry.request.method"
    /// </summary>
    public async Task<FhirOperationResult> GetHistoryAsync(
        HistoryRequest request,
        string selfUrl,
        CancellationToken ct = default)
    {
        IReadOnlyList<HistoryEntry> entries;

        if (request.FhirId != null && request.ResourceType != null)
        {
            var (ok, error, e) = await _versioning.GetInstanceHistoryAsync(
                request.ResourceType, request.FhirId, request.Limit, ct);
            if (!ok)
                return FhirOperationResult.NotFound(
                    FhirResponseHelper.CreateOperationOutcome(Severity.Error, IssueCodes.NotFound, error));
            entries = e;
        }
        else if (request.ResourceType != null)
        {
            var (ok, error, e) = await _versioning.GetTypeHistoryAsync(
                request.ResourceType, request.Limit, request.Since, ct);
            if (!ok)
                return FhirOperationResult.BadRequest(
                    FhirResponseHelper.CreateOperationOutcome(Severity.Error, IssueCodes.Invalid, error));
            entries = e;
        }
        else
        {
            var (ok, error, e) = await _versioning.GetSystemHistoryAsync(request.Limit, request.Since, ct);
            if (!ok)
                return FhirOperationResult.BadRequest(
                    FhirResponseHelper.CreateOperationOutcome(Severity.Error, IssueCodes.Invalid, error));
            entries = e;
        }

        return FhirOperationResult.Ok(FhirResponseHelper.CreateHistoryBundle(entries, selfUrl));
    }

    /// <summary>
    /// Processes a Bundle (batch or transaction).
    /// 
    /// FHIR Spec Reference: §3.2.0.13 batch/transaction (https://build.fhir.org/http.html#transaction)
    /// 
    /// Request: POST [base] with Bundle (type="batch" or "transaction")
    /// 
    /// Transaction behavior (§3.2.0.13.2):
    /// - All actions succeed or all fail together atomically
    /// - Processing order: DELETE ? POST ? PUT/PATCH ? GET
    /// - If any identity overlaps in steps 1-3, transaction SHALL fail
    /// - References within bundle are resolved and updated to server-assigned IDs
    /// 
    /// Batch behavior (§3.2.0.13.1):
    /// - Each entry processed independently
    /// - No interdependencies between entries allowed
    /// - Success/failure of one entry does not affect others
    /// 
    /// Success Response:
    /// - 200 OK: Bundle processed
    ///   - Body: Bundle with type="transaction-response" or "batch-response"
    ///   - Each entry contains response element with status code and location
    /// 
    /// Error Response (Transaction):
    /// - 400/500: Entire transaction failed, return single OperationOutcome
    /// 
    /// Per spec: "For a transaction, servers SHALL either accept all actions and return 200 OK, 
    /// or reject all resources and return HTTP 400 or 500."
    /// 
    /// Per spec: "For a batch, there SHALL be no interdependencies between the different entries 
    /// in the Bundle that cause change on the server."
    /// </summary>
    public async Task<FhirOperationResult> ProcessBundleRequestAsync(
        BundleRequest request,
        CancellationToken ct = default)
    {
        try
        {
            if (!request.BundleDocument.RootElement.TryGetProperty(Properties.ResourceType, out var rt) ||
                rt.GetString() != ResourceTypes.Bundle)
            {
                return FhirOperationResult.BadRequest(
                    FhirResponseHelper.CreateOperationOutcome(Severity.Error, IssueCodes.Invalid, "Expected a Bundle resource"));
            }

            if (!request.BundleDocument.RootElement.TryGetProperty(BundleEntry.Type, out var bundleType))
            {
                return FhirOperationResult.BadRequest(
                    FhirResponseHelper.CreateOperationOutcome(Severity.Error, IssueCodes.Invalid, "Bundle.type is required"));
            }

            var type = bundleType.GetString();
            var (ok, error, responseBundle) = type switch
            {
                BundleTypes.Transaction => await _bundle.ProcessTransactionAsync(request.BundleDocument, ct),
                BundleTypes.Batch => await _bundle.ProcessBatchAsync(request.BundleDocument, ct),
                _ => (false, $"Unsupported bundle type: {type}. Expected 'batch' or 'transaction'", null as JsonDocument)
            };

            if (!ok)
            {
                return type == BundleTypes.Transaction
                    ? FhirOperationResult.BadRequest(
                        FhirResponseHelper.CreateOperationOutcome(Severity.Error, IssueCodes.Invalid, error))
                    : FhirOperationResult.InternalError(
                        FhirResponseHelper.CreateOperationOutcome(Severity.Error, IssueCodes.Exception, error));
            }

            return FhirOperationResult.Ok(
                JsonSerializer.Deserialize<JsonElement>(responseBundle!.RootElement.GetRawText()));
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Bundle processing failed");
            return FhirOperationResult.InternalError(
                FhirResponseHelper.CreateOperationOutcome(Severity.Fatal, IssueCodes.Exception, ex.Message));
        }
    }

    /// <summary>
    /// Validates a resource without persisting.
    /// 
    /// FHIR Spec Reference: Operations framework (https://build.fhir.org/operations.html)
    /// Specifically: $validate operation (https://build.fhir.org/operation-resource-validate.html)
    /// 
    /// Request: POST [base]/[type]/$validate with Resource body
    /// 
    /// Response: OperationOutcome indicating validation results
    /// - severity="information": Validation successful
    /// - severity="error": Validation failed with details
    /// 
    /// This operation validates the resource against the FHIR schema without persisting.
    /// Useful for clients to verify resources before submission.
    /// </summary>
    public FhirOperationResult ValidateResource(JsonDocument json, string resourceType)
    {
        try
        {
            if (json.RootElement.TryGetProperty(Properties.ResourceType, out var rtProp) &&
                rtProp.GetString() != resourceType)
            {
                return FhirOperationResult.Ok(
                    FhirResponseHelper.CreateOperationOutcome(Severity.Error, IssueCodes.Invalid,
                        $"Resource type mismatch: URL specifies {resourceType} but payload has {rtProp.GetString()}"));
            }

            var (ok, error, detectedResourceType, fhirId) = _validation.ValidateOnly(json);

            return FhirOperationResult.Ok(ok
                ? FhirResponseHelper.CreateOperationOutcome(Severity.Information, IssueCodes.Informational,
                    $"Validation successful for {detectedResourceType}" + (fhirId != null ? $"/{fhirId}" : ""))
                : FhirResponseHelper.CreateOperationOutcome(Severity.Error, IssueCodes.Invalid, error));
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Validation failed");
            return FhirOperationResult.InternalError(
                FhirResponseHelper.CreateOperationOutcome(Severity.Fatal, IssueCodes.Exception, ex.Message));
        }
    }

    /// <summary>
    /// Patient $everything operation.
    /// 
    /// FHIR Spec Reference: Patient compartment and $everything operation
    /// See: https://build.fhir.org/patient-operation-everything.html
    /// 
    /// Request: GET [base]/Patient/[id]/$everything
    /// 
    /// Response: Bundle with type="searchset" containing:
    /// - The Patient resource itself
    /// - All resources in the Patient compartment (Observations, Conditions, etc.)
    /// - Resources referenced by compartment resources (up to maxHops)
    /// 
    /// This operation fetches an entire patient record for health record transfer
    /// or summary purposes.
    /// </summary>
    public async Task<FhirOperationResult> GetPatientEverythingBundleAsync(
        string patientId,
        int limit,
        string baseUrl,
        CancellationToken ct = default)
    {
        var (ok, error, result) = await _patient.GetPatientEverythingAsync(patientId, maxHops: 3, limit: limit, ct);

        if (!ok)
            return FhirOperationResult.NotFound(
                FhirResponseHelper.CreateOperationOutcome(Severity.Error, IssueCodes.NotFound, error));

        var bundle = new
        {
            resourceType = ResourceTypes.Bundle,
            type = BundleTypes.SearchSet,
            total = result!.Resources.Count,
            link = new[] { new { relation = Status.Self, url = $"{baseUrl}/{ResourceTypes.Patient}/{patientId}/$everything" } },
            entry = result.Resources.Where(r => r.Json != null).Select(r => new
            {
                fullUrl = $"{baseUrl}/{r.ResourceType}/{r.FhirId}",
                resource = JsonSerializer.Deserialize<JsonElement>(r.Json!)
            }).ToArray()
        };

        return FhirOperationResult.Ok(bundle);
    }

    /// <summary>
    /// Deletes resource history (all versions except current).
    /// 
    /// FHIR Spec Reference: §3.2.0.8 delete-history (https://build.fhir.org/http.html#delete-history)
    /// Note: This is a TRIAL USE feature in FHIR R6.
    /// 
    /// Request: DELETE [base]/[type]/[id]/_history
    /// 
    /// Success Response:
    /// - 200 OK: If response contains payload (our implementation returns count)
    /// - 204 No Content: With no response payload
    /// 
    /// Error Response:
    /// - 404 Not Found: Resource does not exist
    /// - 405 Method Not Allowed: Server refuses to delete history as policy
    /// - 409 Conflict: Business rules prevent deletion
    /// 
    /// Per spec: "The delete history interaction removes all versions of the resource 
    /// except the current version (which if the resource has been deleted, will be 
    /// an empty placeholder)."
    /// 
    /// Per spec: "Subsequent version specific reads of the resource can return a 410 Gone 
    /// HTTP status code when the server wishes to indicate that the resource is deleted, 
    /// or a 404 Not Found HTTP status code when it does not."
    /// </summary>
    public async Task<FhirOperationResult> DeleteResourceHistoryAsync(
        string resourceType,
        string fhirId,
        CancellationToken ct = default)
    {
        var (ok, error, deletedCount) = await _versioning.DeleteHistoryAsync(resourceType, fhirId, ct);

        if (!ok)
            return FhirOperationResult.NotFound(
                FhirResponseHelper.CreateOperationOutcome(Severity.Error, IssueCodes.NotFound, error));

        return FhirOperationResult.Ok(new
        {
            deletedCount,
            message = $"Deleted {deletedCount} version(s) of {resourceType}/{fhirId}"
        });
    }

    /// <summary>
    /// Deletes a specific version from history.
    /// 
    /// FHIR Spec Reference: §3.2.0.9 delete-history-version (https://build.fhir.org/http.html#delete-history-version)
    /// Note: This is a TRIAL USE feature in FHIR R6.
    /// 
    /// Request: DELETE [base]/[type]/[id]/_history/[vid]
    /// 
    /// Success Response:
    /// - 200 OK: If response contains payload
    /// - 204 No Content: With no response payload (our implementation)
    /// 
    /// Error Response:
    /// - 404 Not Found: Version does not exist or resource does not exist
    /// - 405 Method Not Allowed: Server refuses to delete specific versions as policy
    /// - 409 Conflict: Business rules prevent deletion (e.g., cannot delete current version)
    /// 
    /// Per spec: "The delete history version interaction removes a specific historical 
    /// version of the resource, except the current version."
    /// </summary>
    public async Task<FhirOperationResult> DeleteResourceVersionAsync(
        string resourceType,
        string fhirId,
        string versionId,
        CancellationToken ct = default)
    {
        var (ok, error) = await _versioning.DeleteVersionAsync(resourceType, fhirId, versionId, ct);

        return ok
            ? FhirOperationResult.NoContent()
            : FhirOperationResult.NotFound(
                FhirResponseHelper.CreateOperationOutcome(Severity.Error, IssueCodes.NotFound, error));
    }

    /// <summary>
    /// Gets the capability statement describing server functionality.
    /// 
    /// FHIR Spec Reference: §3.2.0.12 capabilities (https://build.fhir.org/http.html#capabilities)
    /// 
    /// Request: GET [base]/metadata
    /// 
    /// Response:
    /// - 200 OK: CapabilityStatement resource describing server capabilities
    /// - ETag SHOULD be returned (per spec)
    /// 
    /// Per spec: "Applications SHALL return a resource that describes the functionality 
    /// of the server end-point."
    /// 
    /// The CapabilityStatement includes:
    /// - fhirVersion: The FHIR version supported
    /// - format: Supported content types (application/fhir+json)
    /// - patchFormat: Supported patch formats (application/json-patch+json)
    /// - rest: RESTful capability details including:
    ///   - Supported resource types
    ///   - Supported interactions per resource (read, vread, update, patch, delete, etc.)
    ///   - Versioning support level
    /// 
    /// Per spec: "Servers SHOULD check for the fhirVersion MIME-type parameter when 
    /// processing this request."
    /// </summary>
    public object GetCapabilityStatement()
    {
        var resourceTypes = _validation.GetSupportedResourceTypes();

        return new
        {
            resourceType = ResourceTypes.CapabilityStatement,
            status = Status.Active,
            date = DateTime.UtcNow.ToString("o"),
            kind = Status.Instance,
            fhirVersion = FhirVersions.R6Ballot3,
            format = new[] { ContentTypes.FhirJson, ContentTypes.Json },
            patchFormat = new[] { ContentTypes.JsonPatch },
            rest = new[]
            {
                new
                {
                    mode = Status.Server,
                    resource = resourceTypes.Select(rt => new
                    {
                        type = rt,
                        interaction = new[]
                        {
                            new { code = InteractionCodes.Read },
                            new { code = InteractionCodes.VRead },
                            new { code = InteractionCodes.Update },
                            new { code = InteractionCodes.Patch },
                            new { code = InteractionCodes.Delete },
                            new { code = InteractionCodes.HistoryInstance },
                            new { code = InteractionCodes.HistoryType },
                            new { code = InteractionCodes.Create },
                            new { code = InteractionCodes.SearchType }
                        },
                        conditionalCreate = true,
                        conditionalUpdate = true,
                        conditionalPatch = true,
                        conditionalDelete = Status.Single,
                        versioning = Status.Versioned,
                        readHistory = true,
                        searchParam = new[]
                        {
                            new { name = SearchParams.Id, type = Status.Token },
                            new { name = SearchParams.Identifier, type = Status.Token }
                        }
                    }).ToArray(),
                    interaction = new[]
                    {
                        new { code = InteractionCodes.Transaction },
                        new { code = InteractionCodes.Batch },
                        new { code = InteractionCodes.SearchSystem },
                        new { code = InteractionCodes.HistorySystem }
                    },
                    operation = new[]
                    {
                        new { name = "validate", definition = "http://hl7.org/fhir/OperationDefinition/Resource-validate" }
                    }
                }
            }
        };
    }
}
