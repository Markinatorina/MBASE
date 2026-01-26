using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using System.Text.Json;
using BLL.Services;
using BLL.Models;
using static BLL.Constants;

namespace API.Controllers
{
    /// <summary>
    /// FHIR RESTful API Controller implementing standard FHIR interactions.
    /// 
    /// Specification Reference: https://build.fhir.org/http.html (FHIR R6 6.0.0-ballot3)
    /// 
    /// This controller implements the FHIR RESTful API, providing endpoints organized into:
    /// 
    /// Whole System Interactions (§3.2.0):
    /// - GET /metadata - capabilities interaction (§3.2.0.12)
    /// - POST / - batch/transaction interaction (§3.2.0.13)
    /// - GET / - search-system interaction (§3.2.0.11)
    /// - GET /_history - history-system interaction (§3.2.0.14)
    /// 
    /// Type Level Interactions:
    /// - POST /[type] - create interaction (§3.2.0.10)
    /// - GET /[type] - search-type interaction (§3.2.0.11)
    /// - DELETE /[type]?[params] - conditional delete (§3.2.0.7.1)
    /// - PUT /[type]?[params] - conditional update (§3.2.0.4.3)
    /// - PATCH /[type]?[params] - conditional patch (§3.2.0.6.1)
    /// - GET /[type]/_history - history-type interaction (§3.2.0.14)
    /// 
    /// Instance Level Interactions:
    /// - GET /[type]/[id] - read interaction (§3.2.0.2)
    /// - PUT /[type]/[id] - update interaction (§3.2.0.4)
    /// - PATCH /[type]/[id] - patch interaction (§3.2.0.6)
    /// - DELETE /[type]/[id] - delete interaction (§3.2.0.7)
    /// - GET /[type]/[id]/_history/[vid] - vread interaction (§3.2.0.3)
    /// - GET /[type]/[id]/_history - history-instance interaction (§3.2.0.14)
    /// - DELETE /[type]/[id]/_history - delete-history (§3.2.0.8, Trial Use)
    /// - DELETE /[type]/[id]/_history/[vid] - delete-history-version (§3.2.0.9, Trial Use)
    /// 
    /// HTTP Headers (§3.2.0.1.6):
    /// - ETag: W/"[versionId]" returned on responses (weak ETag per §3.2.0.1.3)
    /// - If-Match: Version-aware updates (§3.2.0.5)
    /// - If-None-Match: Conditional reads (§3.2.0.1.8)
    /// - If-None-Exist: Conditional create (§3.2.0.10.1)
    /// - Location: Returned on create/update with [base]/[type]/[id]/_history/[vid]
    /// - Last-Modified: Returned from meta.lastUpdated
    /// 
    /// Content Types (§3.2.0.1.10):
    /// - application/fhir+json (primary)
    /// - application/json (accepted)
    /// - application/json-patch+json (for PATCH)
    /// </summary>
    [Route("api/fhir/" + FhirVersions.R6Ballot3 + "/")]
    [ApiController]
    public class FHIRController : ControllerBase
    {
        private readonly FHIRService _service;

        public FHIRController(FHIRService service)
        {
            _service = service;
        }

        private string BaseUrl => $"{Request.Scheme}://{Request.Host}/api/fhir";
        private string SelfUrl => $"{Request.Scheme}://{Request.Host}{Request.Path}{Request.QueryString}";

        /// <summary>
        /// Converts FhirOperationResult to IActionResult with proper HTTP headers.
        /// 
        /// Per FHIR spec §3.2.0.1.6 HTTP Headers:
        /// - Location: Set on 201 Created responses
        /// - ETag: W/"[versionId]" for versioned responses
        /// - Last-Modified: From meta.lastUpdated when available
        /// 
        /// Status code mapping per spec:
        /// - 200 OK: Successful read/update/patch/search/history
        /// - 201 Created: Successful create
        /// - 204 No Content: Successful delete with no payload
        /// - 304 Not Modified: Conditional read with matching ETag
        /// - 4xx/5xx: Error responses with OperationOutcome body
        /// </summary>
        private IActionResult ToActionResult(FhirOperationResult result)
        {
            if (result.Location != null)
                Response.Headers.Location = result.Location;
            if (result.ETag != null)
                Response.Headers.ETag = result.ETag;
            if (result.LastModified != null)
                Response.Headers["Last-Modified"] = result.LastModified;

            return result.StatusCode switch
            {
                200 => result.Body is string json ? Content(json, "application/fhir+json") : Ok(result.Body),
                201 => StatusCode(201, result.Body),
                204 => NoContent(),
                304 => StatusCode(304),
                _ => StatusCode(result.StatusCode, result.Body)
            };
        }

        #region Whole System Interactions

        /// <summary>
        /// capabilities interaction - Returns server's CapabilityStatement.
        /// 
        /// FHIR Spec: §3.2.0.12 (https://build.fhir.org/http.html#capabilities)
        /// 
        /// GET [base]/metadata
        /// 
        /// Returns: CapabilityStatement describing which resource types and 
        /// interactions are supported by this server.
        /// </summary>
        [HttpGet("metadata")]
        public IActionResult GetCapabilities() 
            => Ok(_service.GetCapabilityStatement());

        /// <summary>
        /// batch/transaction interaction - Process multiple operations in a single request.
        /// 
        /// FHIR Spec: §3.2.0.13 (https://build.fhir.org/http.html#transaction)
        /// 
        /// POST [base] with Bundle (type="batch" or "transaction")
        /// 
        /// Transaction: All-or-nothing semantics - all entries succeed or all fail.
        /// Batch: Each entry processed independently.
        /// 
        /// Returns: Bundle with type="transaction-response" or "batch-response"
        /// </summary>
        [HttpPost]
        [Consumes("application/fhir+json", "application/json")]
        public async Task<IActionResult> ProcessBundle([FromBody] JsonElement payload, CancellationToken ct = default)
        {
            using var bundleDoc = JsonDocument.Parse(payload.GetRawText());
            var result = await _service.ProcessBundleRequestAsync(new BundleRequest(bundleDoc), ct);
            return ToActionResult(result);
        }

        /// <summary>
        /// search-system interaction - Search across all resource types.
        /// 
        /// FHIR Spec: §3.2.0.11 search (https://build.fhir.org/http.html#search)
        /// 
        /// GET [base]?[parameters]
        /// 
        /// Parameters:
        /// - _type: Comma-separated list of resource types to search
        /// - _id: Filter by resource id
        /// - _count: Maximum results per page
        /// - _offset: Pagination offset
        /// 
        /// Returns: Bundle with type="searchset"
        /// </summary>
        [HttpGet]
        public async Task<IActionResult> SearchSystem(
            [FromQuery] string? _type = null,
            [FromQuery] string? _id = null,
            [FromQuery] int _count = 100,
            [FromQuery] int _offset = 0,
            CancellationToken ct = default)
        {
            var filters = new Dictionary<string, object>();
            if (!string.IsNullOrWhiteSpace(_id)) filters["id"] = _id;

            var request = new SearchRequest(
                ResourceTypes: string.IsNullOrEmpty(_type) ? null : _type.Split(',').ToList(),
                Filters: filters.Count > 0 ? filters : null,
                Limit: _count,
                Offset: _offset);

            var result = await _service.SearchResourcesAsync(request, SelfUrl, BaseUrl, ct);
            return ToActionResult(result);
        }

        /// <summary>
        /// search-system via POST - Alternative POST-based system search.
        /// 
        /// FHIR Spec: §3.2.0.11 (https://build.fhir.org/http.html#search)
        /// 
        /// POST [base]/_search with form data
        /// 
        /// Per spec: "Servers supporting Search via HTTP SHALL support both modes of operation"
        /// </summary>
        [HttpPost("_search")]
        public Task<IActionResult> SearchSystemPost(
            [FromForm] string? _type = null,
            [FromForm] string? _id = null,
            [FromForm] int _count = 100,
            [FromForm] int _offset = 0,
            CancellationToken ct = default)
            => SearchSystem(_type, _id, _count, _offset, ct);

        /// <summary>
        /// history-system interaction - Retrieve change history for all resources.
        /// 
        /// FHIR Spec: §3.2.0.14 (https://build.fhir.org/http.html#history)
        /// 
        /// GET [base]/_history
        /// 
        /// Parameters:
        /// - _count: Maximum entries per page
        /// - _since: Only include versions created at or after the given instant
        /// 
        /// Returns: Bundle with type="history", sorted oldest versions last
        /// </summary>
        [HttpGet("_history")]
        public async Task<IActionResult> HistorySystem(
            [FromQuery] int _count = 100,
            [FromQuery] string? _since = null,
            CancellationToken ct = default)
        {
            DateTime? since = null;
            if (!string.IsNullOrEmpty(_since) && DateTime.TryParse(_since, out var parsed))
                since = parsed;

            var result = await _service.GetHistoryAsync(
                new HistoryRequest(Limit: _count, Since: since),
                $"{BaseUrl}/_history", ct);
            return ToActionResult(result);
        }

        #endregion

        #region Type Level Interactions

        /// <summary>
        /// create interaction - Create a new resource with server-assigned id.
        /// 
        /// FHIR Spec: §3.2.0.10 (https://build.fhir.org/http.html#create)
        /// 
        /// POST [base]/[type]
        /// 
        /// Headers:
        /// - If-None-Exist: Conditional create (§3.2.0.10.1) - prevents duplicates
        /// 
        /// Success: 201 Created with Location header
        /// Conditional match: 200 OK (existing resource)
        /// Multiple matches: 412 Precondition Failed
        /// </summary>
        [HttpPost("{resourceType}")]
        public async Task<IActionResult> Create(
            [FromRoute] string resourceType,
            [FromBody] JsonElement payload,
            [FromQuery] bool materializeReferences = false,
            [FromQuery] bool allowPlaceholderTargets = false,
            [FromHeader(Name = "If-None-Exist")] string? ifNoneExist = null,
            CancellationToken ct = default)
        {
            using var json = JsonDocument.Parse(payload.GetRawText());
            var request = new CreateResourceRequest(json, resourceType, materializeReferences, allowPlaceholderTargets, ifNoneExist);
            var result = await _service.CreateResourceAsync(request, BaseUrl, ct);
            return ToActionResult(result);
        }

        /// <summary>
        /// conditional delete - Delete resources matching search criteria.
        /// 
        /// FHIR Spec: §3.2.0.7.1 (https://build.fhir.org/http.html#cdelete)
        /// 
        /// DELETE [base]/[type]?[search parameters]
        /// 
        /// Using delete-conditional-single: Returns 412 if multiple matches found.
        /// No matches: 404 Not Found
        /// One match: 204 No Content
        /// </summary>
        [HttpDelete("{resourceType}")]
        public async Task<IActionResult> ConditionalDelete(
            [FromRoute] string resourceType,
            [FromQuery] string? identifier = null,
            [FromQuery] string? _id = null,
            CancellationToken ct = default)
        {
            var criteria = new Dictionary<string, object>();
            if (!string.IsNullOrEmpty(identifier)) criteria["identifier"] = identifier;
            if (!string.IsNullOrEmpty(_id)) criteria["id"] = _id;

            var result = await _service.ConditionalDeleteResourceAsync(new ConditionalRequest(resourceType, criteria), ct);
            return ToActionResult(result);
        }

        /// <summary>
        /// conditional update - Update resource matching search criteria.
        /// 
        /// FHIR Spec: §3.2.0.4.3 (https://build.fhir.org/http.html#cond-update)
        /// 
        /// PUT [base]/[type]?[search parameters]
        /// 
        /// No matches: Create resource (200/201)
        /// One match: Update resource (200)
        /// Multiple matches: 412 Precondition Failed
        /// </summary>
        [HttpPut("{resourceType}")]
        public async Task<IActionResult> ConditionalUpdate(
            [FromRoute] string resourceType,
            [FromBody] JsonElement payload,
            [FromQuery] string? identifier = null,
            [FromQuery] string? _id = null,
            [FromQuery] bool materializeReferences = false,
            [FromQuery] bool allowPlaceholderTargets = false,
            CancellationToken ct = default)
        {
            var criteria = new Dictionary<string, object>();
            if (!string.IsNullOrEmpty(identifier)) criteria["identifier"] = identifier;
            if (!string.IsNullOrEmpty(_id)) criteria["id"] = _id;

            using var json = JsonDocument.Parse(payload.GetRawText());
            var result = await _service.ConditionalUpdateResourceAsync(
                json, new ConditionalRequest(resourceType, criteria),
                materializeReferences, allowPlaceholderTargets, BaseUrl, ct);
            return ToActionResult(result);
        }

        /// <summary>
        /// conditional patch - Patch resource matching search criteria.
        /// 
        /// FHIR Spec: §3.2.0.6.1 (https://build.fhir.org/http.html#cond-patch)
        /// 
        /// PATCH [base]/[type]?[search parameters]
        /// Content-Type: application/json-patch+json
        /// 
        /// No matches: 404 Not Found
        /// One match: Apply patch, return 200
        /// Multiple matches: 412 Precondition Failed
        /// </summary>
        [HttpPatch("{resourceType}")]
        [Consumes("application/json-patch+json")]
        public async Task<IActionResult> ConditionalPatch(
            [FromRoute] string resourceType,
            [FromBody] JsonElement payload,
            [FromQuery] string? identifier = null,
            [FromQuery] string? _id = null,
            CancellationToken ct = default)
        {
            var criteria = new Dictionary<string, object>();
            if (!string.IsNullOrEmpty(identifier)) criteria["identifier"] = identifier;
            if (!string.IsNullOrEmpty(_id)) criteria["id"] = _id;

            using var patchDoc = JsonDocument.Parse(payload.GetRawText());
            var result = await _service.ConditionalPatchResourceAsync(
                patchDoc, new ConditionalRequest(resourceType, criteria), ct);
            return ToActionResult(result);
        }

        /// <summary>
        /// search-type interaction - Search resources of a specific type.
        /// 
        /// FHIR Spec: §3.2.0.11 (https://build.fhir.org/http.html#search)
        /// 
        /// GET [base]/[type]?[parameters]
        /// 
        /// Returns: Bundle with type="searchset"
        /// </summary>
        [HttpGet("{resourceType}")]
        public async Task<IActionResult> SearchType(
            [FromRoute] string resourceType,
            [FromQuery] string? _id = null,
            [FromQuery] string? identifier = null,
            [FromQuery] int _count = 100,
            [FromQuery] int _offset = 0,
            CancellationToken ct = default)
        {
            var filters = new Dictionary<string, object>();
            if (!string.IsNullOrWhiteSpace(_id)) filters["id"] = _id;
            if (!string.IsNullOrWhiteSpace(identifier)) filters["identifier"] = identifier;

            var request = new SearchRequest(resourceType, Filters: filters.Count > 0 ? filters : null, Limit: _count, Offset: _offset);
            var result = await _service.SearchResourcesAsync(request, SelfUrl, BaseUrl, ct);
            return ToActionResult(result);
        }

        /// <summary>
        /// search-type via POST - Alternative POST-based type search.
        /// 
        /// FHIR Spec: §3.2.0.11 (https://build.fhir.org/http.html#search)
        /// 
        /// POST [base]/[type]/_search with form data
        /// </summary>
        [HttpPost("{resourceType}/_search")]
        public Task<IActionResult> SearchTypePost(
            [FromRoute] string resourceType,
            [FromForm] string? _id = null,
            [FromForm] string? identifier = null,
            [FromForm] int _count = 100,
            [FromForm] int _offset = 0,
            CancellationToken ct = default)
            => SearchType(resourceType, _id, identifier, _count, _offset, ct);

        #endregion

        #region Instance Level Interactions

        /// <summary>
        /// read interaction - Read the current state of a resource.
        /// 
        /// FHIR Spec: §3.2.0.2 (https://build.fhir.org/http.html#read)
        /// 
        /// GET [base]/[type]/[id]
        /// 
        /// Headers:
        /// - If-None-Match: Conditional read (§3.2.0.1.8) - returns 304 if ETag matches
        /// 
        /// Success: 200 OK with resource body and ETag header
        /// Not found: 404 Not Found
        /// Deleted: 410 Gone
        /// Conditional match: 304 Not Modified
        /// </summary>
        [HttpGet("{resourceType}/{fhirId}")]
        public async Task<IActionResult> Read(
            [FromRoute] string resourceType,
            [FromRoute] string fhirId,
            [FromHeader(Name = "If-None-Match")] string? ifNoneMatch = null,
            CancellationToken ct = default)
        {
            var result = await _service.ReadResourceAsync(resourceType, fhirId, ifNoneMatch, ct);
            return ToActionResult(result);
        }

        /// <summary>
        /// update interaction - Update an existing resource by its id.
        /// 
        /// FHIR Spec: §3.2.0.4 (https://build.fhir.org/http.html#update)
        /// 
        /// PUT [base]/[type]/[id]
        /// 
        /// Headers:
        /// - If-Match: Version-aware update (§3.2.0.5) - prevents lost updates
        /// 
        /// Requirements per spec:
        /// - Resource id MUST match URL id (400 Bad Request if mismatch)
        /// - Server ignores meta.versionId and meta.lastUpdated in request
        /// 
        /// Success: 200 OK with Location and ETag headers
        /// Version conflict: 412 Precondition Failed
        /// </summary>
        [HttpPut("{resourceType}/{fhirId}")]
        public async Task<IActionResult> Update(
            [FromRoute] string resourceType,
            [FromRoute] string fhirId,
            [FromBody] JsonElement payload,
            [FromQuery] bool materializeReferences = false,
            [FromQuery] bool allowPlaceholderTargets = false,
            [FromHeader(Name = "If-Match")] string? ifMatch = null,
            CancellationToken ct = default)
        {
            using var json = JsonDocument.Parse(payload.GetRawText());
            var request = new UpdateResourceRequest(json, resourceType, fhirId, materializeReferences, allowPlaceholderTargets, ifMatch);
            var result = await _service.UpdateResourceAsync(request, BaseUrl, ct);
            return ToActionResult(result);
        }

        /// <summary>
        /// patch interaction - Partial update of a resource.
        /// 
        /// FHIR Spec: §3.2.0.6 (https://build.fhir.org/http.html#patch)
        /// 
        /// PATCH [base]/[type]/[id]
        /// Content-Type: application/json-patch+json (JSON Patch per RFC 6902)
        /// 
        /// Headers:
        /// - If-Match: Version-aware patch (§3.2.0.5) - SHOULD be used per spec
        /// 
        /// Per spec: "Processing PATCH operations may be very version sensitive. 
        /// Servers that support PATCH SHALL support Resource Contention."
        /// 
        /// Success: 200 OK with ETag header
        /// Version conflict: 412 Precondition Failed
        /// Patch failure: 422 Unprocessable Entity
        /// </summary>
        [HttpPatch("{resourceType}/{fhirId}")]
        [Consumes("application/json-patch+json")]
        public async Task<IActionResult> Patch(
            [FromRoute] string resourceType,
            [FromRoute] string fhirId,
            [FromBody] JsonElement payload,
            [FromHeader(Name = "If-Match")] string? ifMatch = null,
            CancellationToken ct = default)
        {
            using var patchDoc = JsonDocument.Parse(payload.GetRawText());
            var request = new PatchResourceRequest(patchDoc, resourceType, fhirId, ifMatch);
            var result = await _service.PatchResourceAsync(request, ct);
            return ToActionResult(result);
        }

        /// <summary>
        /// delete interaction - Delete a resource.
        /// 
        /// FHIR Spec: §3.2.0.7 (https://build.fhir.org/http.html#delete)
        /// 
        /// DELETE [base]/[type]/[id]
        /// 
        /// Headers:
        /// - If-Match: Version-aware delete (§3.2.0.5)
        /// 
        /// Per spec: "A delete interaction means that subsequent non-version specific 
        /// reads of the resource return a 410 Gone HTTP status code."
        /// 
        /// Success: 204 No Content (or 200 OK with payload)
        /// Version conflict: 412 Precondition Failed
        /// 
        /// Note: Deleted resources may be restored via subsequent PUT (update).
        /// </summary>
        [HttpDelete("{resourceType}/{fhirId}")]
        public async Task<IActionResult> Delete(
            [FromRoute] string resourceType,
            [FromRoute] string fhirId,
            [FromHeader(Name = "If-Match")] string? ifMatch = null,
            CancellationToken ct = default)
        {
            var request = new DeleteResourceRequest(resourceType, fhirId, ifMatch);
            var result = await _service.DeleteResourceAsync(request, ct);
            return ToActionResult(result);
        }

        /// <summary>
        /// vread interaction - Read a specific version of a resource.
        /// 
        /// FHIR Spec: §3.2.0.3 (https://build.fhir.org/http.html#vread)
        /// 
        /// GET [base]/[type]/[id]/_history/[vid]
        /// 
        /// Per spec: "The returned resource SHALL have an id element with a value 
        /// that is the [id], and a meta.versionId element with a value of [vid]."
        /// 
        /// Success: 200 OK with ETag and Last-Modified headers
        /// Version deleted: 410 Gone
        /// Version not found: 404 Not Found
        /// </summary>
        [HttpGet("{resourceType}/{fhirId}/_history/{versionId}")]
        public async Task<IActionResult> VRead(
            [FromRoute] string resourceType,
            [FromRoute] string fhirId,
            [FromRoute] string versionId,
            CancellationToken ct = default)
        {
            var result = await _service.ReadVersionAsync(resourceType, fhirId, versionId, ct);
            return ToActionResult(result);
        }

        /// <summary>
        /// history-instance interaction - Retrieve history for a specific resource.
        /// 
        /// FHIR Spec: §3.2.0.14 (https://build.fhir.org/http.html#history)
        /// 
        /// GET [base]/[type]/[id]/_history
        /// 
        /// Returns: Bundle with type="history" containing all versions of the resource,
        /// sorted with oldest versions last, including deleted versions.
        /// 
        /// Each entry includes request (method/url) to differentiate creates/updates/deletes.
        /// </summary>
        [HttpGet("{resourceType}/{fhirId}/_history")]
        public async Task<IActionResult> HistoryInstance(
            [FromRoute] string resourceType,
            [FromRoute] string fhirId,
            [FromQuery] int _count = 100,
            CancellationToken ct = default)
        {
            var result = await _service.GetHistoryAsync(
                new HistoryRequest(resourceType, fhirId, _count),
                $"{BaseUrl}/{resourceType}/{fhirId}/_history", ct);
            return ToActionResult(result);
        }

        /// <summary>
        /// history-type interaction - Retrieve history for all resources of a type.
        /// 
        /// FHIR Spec: §3.2.0.14 (https://build.fhir.org/http.html#history)
        /// 
        /// GET [base]/[type]/_history
        /// 
        /// Parameters:
        /// - _count: Maximum entries per page
        /// - _since: Only include versions created at or after the given instant
        /// 
        /// Returns: Bundle with type="history"
        /// </summary>
        [HttpGet("{resourceType}/_history")]
        public async Task<IActionResult> HistoryType(
            [FromRoute] string resourceType,
            [FromQuery] int _count = 100,
            [FromQuery] string? _since = null,
            CancellationToken ct = default)
        {
            DateTime? since = null;
            if (!string.IsNullOrEmpty(_since) && DateTime.TryParse(_since, out var parsed))
                since = parsed;

            var result = await _service.GetHistoryAsync(
                new HistoryRequest(resourceType, Limit: _count, Since: since),
                $"{BaseUrl}/{resourceType}/_history", ct);
            return ToActionResult(result);
        }

        /// <summary>
        /// delete-history interaction - Delete all historical versions of a resource.
        /// 
        /// FHIR Spec: §3.2.0.8 (https://build.fhir.org/http.html#delete-history)
        /// Note: This is a TRIAL USE feature in FHIR R6.
        /// 
        /// DELETE [base]/[type]/[id]/_history
        /// 
        /// Removes all versions except the current version. If the current version
        /// is a deletion marker, it is preserved.
        /// 
        /// Success: 200 OK with count of deleted versions
        /// Not found: 404 Not Found
        /// </summary>
        [HttpDelete("{resourceType}/{fhirId}/_history")]
        public async Task<IActionResult> DeleteHistory(
            [FromRoute] string resourceType,
            [FromRoute] string fhirId,
            CancellationToken ct = default)
        {
            var result = await _service.DeleteResourceHistoryAsync(resourceType, fhirId, ct);
            return ToActionResult(result);
        }

        /// <summary>
        /// delete-history-version interaction - Delete a specific historical version.
        /// 
        /// FHIR Spec: §3.2.0.9 (https://build.fhir.org/http.html#delete-history-version)
        /// Note: This is a TRIAL USE feature in FHIR R6.
        /// 
        /// DELETE [base]/[type]/[id]/_history/[vid]
        /// 
        /// Removes a specific historical version (cannot delete current version).
        /// 
        /// Success: 204 No Content
        /// Not found: 404 Not Found
        /// Cannot delete current: 409 Conflict
        /// </summary>
        [HttpDelete("{resourceType}/{fhirId}/_history/{versionId}")]
        public async Task<IActionResult> DeleteHistoryVersion(
            [FromRoute] string resourceType,
            [FromRoute] string fhirId,
            [FromRoute] string versionId,
            CancellationToken ct = default)
        {
            var result = await _service.DeleteResourceVersionAsync(resourceType, fhirId, versionId, ct);
            return ToActionResult(result);
        }

        #endregion

        #region Extended Operations

        /// <summary>
        /// $validate operation - Validate a resource without persisting.
        /// 
        /// FHIR Spec: https://build.fhir.org/operation-resource-validate.html
        /// 
        /// POST [base]/[type]/$validate
        /// 
        /// Validates the resource against the FHIR schema and profiles without 
        /// storing it. Useful for pre-submission validation.
        /// 
        /// Returns: OperationOutcome with validation results
        /// - severity="information": Validation successful
        /// - severity="error": Validation failed with details
        /// </summary>
        [HttpPost("{resourceType}/$validate")]
        public IActionResult Validate([FromRoute] string resourceType, [FromBody] JsonElement payload)
        {
            using var json = JsonDocument.Parse(payload.GetRawText());
            var result = _service.ValidateResource(json, resourceType);
            return ToActionResult(result);
        }

        /// <summary>
        /// Patient $everything operation - Fetch entire patient record.
        /// 
        /// FHIR Spec: https://build.fhir.org/patient-operation-everything.html
        /// 
        /// GET [base]/Patient/[id]/$everything
        /// 
        /// Retrieves all resources in the Patient compartment plus referenced resources.
        /// Useful for patient record transfer or summary generation.
        /// 
        /// Parameters:
        /// - _count: Maximum resources to return (default 500)
        /// 
        /// Returns: Bundle with type="searchset" containing patient and related resources
        /// </summary>
        [HttpGet("Patient/{patientId}/$everything")]
        public async Task<IActionResult> PatientEverything(
            [FromRoute] string patientId,
            [FromQuery] int _count = 500,
            CancellationToken ct = default)
        {
            var result = await _service.GetPatientEverythingBundleAsync(patientId, _count, BaseUrl, ct);
            return ToActionResult(result);
        }

        #endregion
    }
}
