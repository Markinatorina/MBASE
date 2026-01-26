using Microsoft.AspNetCore.Mvc;
using System.Text.Json;
using BLL.Services;
using BLL.Models;

namespace API.Controllers
{
    /// <summary>
    /// Graph-specific operations for exploring and manipulating the underlying graph structure.
    /// </summary>
    [Route("api/[controller]")]
    [ApiController]
    public class GraphController : ControllerBase
    {
        private readonly GraphOpsService _service;

        public GraphController(GraphOpsService service)
        {
            _service = service;
        }

        private IActionResult ToActionResult(OperationResult result)
        {
            return result.StatusCode switch
            {
                200 => result.Body is string json ? Content(json, "application/fhir+json") : Ok(result.Body),
                201 => StatusCode(201, result.Body),
                204 => NoContent(),
                _ => StatusCode(result.StatusCode, result.Body)
            };
        }

        [HttpGet("{resourceType}/{fhirId}/references")]
        public async Task<IActionResult> GetReferences(
            [FromRoute] string resourceType,
            [FromRoute] string fhirId,
            CancellationToken ct)
            => ToActionResult(await _service.GetOutgoingReferencesAsync(resourceType, fhirId, ct));

        [HttpGet("{resourceType}/{fhirId}/referrers")]
        public async Task<IActionResult> GetReferrers(
            [FromRoute] string resourceType,
            [FromRoute] string fhirId,
            CancellationToken ct)
            => ToActionResult(await _service.GetIncomingReferencesAsync(resourceType, fhirId, ct));

        [HttpGet("{resourceType}/{fhirId}/traverse")]
        public async Task<IActionResult> Traverse(
            [FromRoute] string resourceType,
            [FromRoute] string fhirId,
            [FromQuery] int maxHops = 2,
            [FromQuery] int limit = 100,
            CancellationToken ct = default)
            => ToActionResult(await _service.TraverseFromResourceAsync(resourceType, fhirId, maxHops, limit, ct));

        [HttpGet("vertex/{id}")]
        public async Task<IActionResult> GetByGraphId([FromRoute] string id, CancellationToken ct)
            => ToActionResult(await _service.GetJsonByGraphIdAsync(id, ct));

        [HttpGet("vertex/{id}/edges")]
        public async Task<IActionResult> GetEdges([FromRoute] string id, CancellationToken ct)
            => ToActionResult(await _service.GetEdgesForVertexAsync(id, ct));

        [HttpPatch("vertex/{id}")]
        public async Task<IActionResult> PatchByGraphId(
            [FromRoute] string id,
            [FromBody] JsonElement payload,
            CancellationToken ct)
        {
            var properties = new Dictionary<string, object> { ["json"] = payload.GetRawText() };
            return ToActionResult(await _service.UpdateByGraphIdAsync(id, properties, ct));
        }

        [HttpDelete("vertex/{id}")]
        public async Task<IActionResult> DeleteByGraphId([FromRoute] string id, CancellationToken ct)
            => ToActionResult(await _service.DeleteByGraphIdAsync(id, ct));

        [HttpPost("edge")]
        public async Task<IActionResult> CreateEdge([FromBody] CreateEdgeRequest request, CancellationToken ct)
        {
            if (request is null)
                return BadRequest(new { error = "Invalid request" });
            return ToActionResult(await _service.CreateEdgeAsync(request, ct));
        }
    }
}
