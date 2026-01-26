namespace BLL;

/// <summary>
/// Centralized constants for the BLL layer to eliminate magic strings.
/// </summary>
public static class Constants
{
    /// <summary>
    /// FHIR version identifiers.
    /// </summary>
    public static class FhirVersions
    {
        public const string R6Ballot3 = "6.0.0-ballot3";
        public const string R5 = "5.0.0";
    }

    /// <summary>
    /// FHIR resource property names stored in graph vertices.
    /// </summary>
    public static class Properties
    {
        public const string ResourceType = "resourceType";
        public const string Json = "json";
        public const string Id = "id";
        public const string IsPlaceholder = "isPlaceholder";
        public const string IsDeleted = "isDeleted";
        public const string IsCurrent = "isCurrent";
        public const string VersionId = "versionId";
        public const string LastUpdated = "lastUpdated";
        public const string Reference = "reference";
    }

    /// <summary>
    /// FHIR edge property names.
    /// </summary>
    public static class EdgeProperties
    {
        public const string Path = "path";
        public const string TargetResourceType = "targetResourceType";
        public const string TargetFhirId = "targetFhirId";
    }

    /// <summary>
    /// Graph edge directions.
    /// </summary>
    public static class EdgeDirection
    {
        public const string Out = "out";
        public const string In = "in";
    }

    /// <summary>
    /// Edge label prefixes and patterns.
    /// </summary>
    public static class EdgeLabels
    {
        public const string FhirReferencePrefix = "fhir:ref:";
    }

    /// <summary>
    /// FHIR resource type names.
    /// </summary>
    public static class ResourceTypes
    {
        public const string Bundle = "Bundle";
        public const string Patient = "Patient";
        public const string CapabilityStatement = "CapabilityStatement";
        public const string OperationOutcome = "OperationOutcome";
    }

    /// <summary>
    /// FHIR Bundle type values.
    /// </summary>
    public static class BundleTypes
    {
        public const string Transaction = "transaction";
        public const string Batch = "batch";
        public const string TransactionResponse = "transaction-response";
        public const string BatchResponse = "batch-response";
        public const string SearchSet = "searchset";
        public const string History = "history";
    }

    /// <summary>
    /// FHIR Bundle entry property names.
    /// </summary>
    public static class BundleEntry
    {
        public const string Entry = "entry";
        public const string Request = "request";
        public const string Resource = "resource";
        public const string FullUrl = "fullUrl";
        public const string Method = "method";
        public const string Url = "url";
        public const string Type = "type";
    }

    /// <summary>
    /// HTTP method names.
    /// </summary>
    public static class HttpMethods
    {
        public const string Get = "GET";
        public const string Post = "POST";
        public const string Put = "PUT";
        public const string Delete = "DELETE";
        public const string Patch = "PATCH";
    }

    /// <summary>
    /// FHIR content types.
    /// </summary>
    public static class ContentTypes
    {
        public const string FhirJson = "application/fhir+json";
        public const string Json = "application/json";
        public const string JsonPatch = "application/json-patch+json";
    }

    /// <summary>
    /// FHIR OperationOutcome severity levels.
    /// </summary>
    public static class Severity
    {
        public const string Fatal = "fatal";
        public const string Error = "error";
        public const string Warning = "warning";
        public const string Information = "information";
    }

    /// <summary>
    /// FHIR OperationOutcome issue codes.
    /// </summary>
    public static class IssueCodes
    {
        public const string Invalid = "invalid";
        public const string NotFound = "not-found";
        public const string Deleted = "deleted";
        public const string Duplicate = "duplicate";
        public const string Conflict = "conflict";
        public const string MultipleMatches = "multiple-matches";
        public const string Exception = "exception";
        public const string Informational = "informational";
    }

    /// <summary>
    /// FHIR schema property names.
    /// </summary>
    public static class SchemaProperties
    {
        public const string Discriminator = "discriminator";
        public const string Mapping = "mapping";
    }

    /// <summary>
    /// Boolean string values.
    /// </summary>
    public static class BooleanStrings
    {
        public const string True = "true";
        public const string False = "false";
    }

    /// <summary>
    /// HTTP status codes as strings (for bundle responses).
    /// </summary>
    public static class StatusCodes
    {
        public const string Ok = "200";
        public const string Created = "201";
        public const string NoContent = "204";
        public const string BadRequest = "400";
        public const string NotFound = "404";
        public const string MethodNotAllowed = "405";
        public const string UnprocessableEntity = "422";
        public const string InternalServerError = "500";
        public const string NotImplemented = "501";
    }

    /// <summary>
    /// FHIR interaction codes for CapabilityStatement.
    /// </summary>
    public static class InteractionCodes
    {
        public const string Read = "read";
        public const string VRead = "vread";
        public const string Update = "update";
        public const string Patch = "patch";
        public const string Delete = "delete";
        public const string HistoryInstance = "history-instance";
        public const string HistoryType = "history-type";
        public const string Create = "create";
        public const string SearchType = "search-type";
        public const string Transaction = "transaction";
        public const string Batch = "batch";
        public const string SearchSystem = "search-system";
        public const string HistorySystem = "history-system";
    }

    /// <summary>
    /// FHIR search parameter names.
    /// </summary>
    public static class SearchParams
    {
        public const string Id = "_id";
        public const string Identifier = "identifier";
    }

    /// <summary>
    /// Common status values for API responses.
    /// </summary>
    public static class Status
    {
        public const string Ok = "ok";
        public const string Wiped = "wiped";
        public const string Active = "active";
        public const string Server = "server";
        public const string Instance = "instance";
        public const string Versioned = "versioned";
        public const string Single = "single";
        public const string Self = "self";
        public const string Match = "match";
        public const string Token = "token";
    }
}
