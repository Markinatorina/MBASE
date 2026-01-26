using BLL.Models;
using DAL.Repositories;
using Microsoft.Extensions.Logging;
using static BLL.Constants;

namespace BLL.Services;

/// <summary>
/// Service for Patient-specific FHIR operations.
/// </summary>
public class FhirPatientService
{
    private readonly IGraphRepository _repo;
    private readonly ILogger<FhirPatientService> _logger;
    private readonly FhirPersistenceService _persistence;

    public FhirPatientService(
        IGraphRepository repo,
        ILogger<FhirPatientService> logger,
        FhirPersistenceService persistence)
    {
        _repo = repo;
        _logger = logger;
        _persistence = persistence;
    }

    /// <summary>
    /// Gets all resources related to a Patient (Patient/$everything operation).
    /// </summary>
    public async Task<(bool ok, string? error, PatientEverythingResult? result)> GetPatientEverythingAsync(
        string patientId,
        int maxHops = 3,
        int limit = 500,
        CancellationToken ct = default)
    {
        var (patientOk, patientError, patientJson, patientGraphId) = 
            await _persistence.GetByResourceTypeAndIdAsync(ResourceTypes.Patient, patientId, ct);
        
        if (!patientOk || patientGraphId is null)
            return (false, patientError ?? "Patient not found", null);

        var vertices = await _repo.TraverseAsync(patientGraphId, maxHops, edgeLabelFilter: null, limit, ct);

        var resources = new List<FhirSearchResult>
        {
            new(patientGraphId, patientId, ResourceTypes.Patient, patientJson, false)
        };

        foreach (var v in vertices)
        {
            var json = v.Properties?.TryGetValue(Properties.Json, out var j) == true ? j?.ToString() : null;
            var fhirId = v.Properties?.TryGetValue(Properties.Id, out var id) == true ? id?.ToString() : null;
            var resourceType = v.Label;
            var isPlaceholder = v.Properties?.TryGetValue(Properties.IsPlaceholder, out var ph) == true &&
                                (ph?.ToString()?.Equals(BooleanStrings.True, StringComparison.OrdinalIgnoreCase) == true || ph is true);

            if (!isPlaceholder && json is not null)
            {
                resources.Add(new FhirSearchResult(v.Id, fhirId, resourceType, json, false));
            }
        }

        return (true, null, new PatientEverythingResult(patientId, resources));
    }
}
