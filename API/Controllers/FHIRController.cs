using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using System.Text.Json;
using BLL.Services;
using DAL.Repositories;

namespace API.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class FHIRController : ControllerBase
    {
        private readonly FHIRService _service;

        public FHIRController(FHIRService service)
        {
            _service = service;
        }

        // POST api/fhir -> accept FHIR JSON resource, validate and persist to JanusGraph.
        // Returns both the internal graph vertex id and the FHIR id (if present).
        [HttpPost]
        public async Task<IActionResult> Post([FromBody] JsonElement payload)
        {
            try
            {
                using var json = JsonDocument.Parse(payload.GetRawText());
                var (ok, error, graphId, fhirId) =
                    await _service.ValidateAndPersistAsync(json);

                if (!ok)
                {
                    return BadRequest(new { error });
                }

                return Ok(new
                {
                    graphId,
                    fhirId
                });
            }
            catch (Exception ex)
            {
                return StatusCode(
                    StatusCodes.Status500InternalServerError,
                    new { error = ex.Message });
            }
        }

        // GET api/fhir/{id} -> retrieve stored JSON by graph vertex id.
        [HttpGet("{id}")]
        public async Task<IActionResult> GetById([FromRoute] string id)
        {
            var (ok, error, json) = await _service.GetAsync(id);
            if (!ok)
            {
                return NotFound(new { error });
            }

            if (json is null)
            {
                return NoContent();
            }

            return Content(json, "application/json");
        }

        // PATCH api/fhir/{id} -> update stored JSON (and revalidate) by graph vertex id.
        // Returns both the graph id and the FHIR id for convenience.
        [HttpPatch("{id}")]
        public async Task<IActionResult> Patch(
            [FromRoute] string id,
            [FromBody] JsonElement payload)
        {
            using var json = JsonDocument.Parse(payload.GetRawText());
            var (ok, error, fhirId) = await _service.UpdateAsync(id, json);
            if (!ok)
            {
                return BadRequest(new { error });
            }

            return Ok(new
            {
                graphId = id,
                fhirId
            });
        }

        // DELETE api/fhir/{id} -> delete stored JSON by graph vertex id.
        [HttpDelete("{id}")]
        public async Task<IActionResult> Delete([FromRoute] string id)
        {
            var (ok, error) = await _service.DeleteAsync(id);
            return ok
                ? NoContent()
                : BadRequest(new { error });
        }

        // POST api/fhir/link -> create an edge between two vertices by properties.
        [HttpPost("link")]
        public async Task<IActionResult> Link([FromBody] LinkRequest request)
        {
            if (request is null)
            {
                return BadRequest(new { error = "Invalid request" });
            }

            var (ok, error, edgeLabel) = await _service.LinkAsync(
                request.Label,
                request.OutLabel,
                request.OutKey,
                request.OutValue,
                request.InLabel,
                request.InKey,
                request.InValue,
                request.Properties ?? new Dictionary<string, object>());

            if (!ok)
            {
                return BadRequest(new { error });
            }

            return Ok(new { label = edgeLabel });
        }

        public class LinkRequest
        {
            public string Label { get; set; } = string.Empty; // edge label
            public string OutLabel { get; set; } = string.Empty;
            public string OutKey { get; set; } = string.Empty;
            public object OutValue { get; set; } = string.Empty;
            public string InLabel { get; set; } = string.Empty;
            public string InKey { get; set; } = string.Empty;
            public object InValue { get; set; } = string.Empty;
            public Dictionary<string, object>? Properties { get; set; }
        }
    }
}