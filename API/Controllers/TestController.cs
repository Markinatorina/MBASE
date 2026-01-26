using Microsoft.AspNetCore.Mvc;
using System.Text.Json;
using BLL.Services;
using BLL.Models;

namespace API.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class TestController : ControllerBase
    {
        private readonly TestOpsService _service;

        public TestController(TestOpsService service)
        {
            _service = service;
        }

        private IActionResult ToActionResult(OperationResult result)
            => result.StatusCode switch
            {
                201 => StatusCode(201, result.Body),
                204 => NoContent(),
                _ => StatusCode(result.StatusCode, result.Body)
            };

        [HttpGet("ping")]
        public async Task<IActionResult> Ping()
            => ToActionResult(await _service.PingAsync());

        [HttpPost("vertex/{label}")]
        public async Task<IActionResult> CreateVertex(
            [FromRoute] string label,
            [FromBody] Dictionary<string, object>? properties)
            => ToActionResult(await _service.CreateVertexAsync(label, properties));

        [HttpGet("vertex/{id}")]
        public async Task<IActionResult> GetVertex([FromRoute] string id)
            => ToActionResult(await _service.GetVertexAsync(id));

        [HttpPatch("vertex/{id}")]
        public async Task<IActionResult> UpdateVertex(
            [FromRoute] string id,
            [FromBody] Dictionary<string, object> properties)
            => ToActionResult(await _service.UpdateVertexPropertiesAsync(id, properties));

        [HttpDelete("vertex/{id}")]
        public async Task<IActionResult> DeleteVertex([FromRoute] string id)
            => ToActionResult(await _service.DeleteVertexAsync(id));

        [HttpDelete("wipe")]
        public async Task<IActionResult> WipeGraph()
            => ToActionResult(await _service.WipeGraphAsync());

        [HttpPost("parse-references")]
        public IActionResult ParseReferences([FromBody] JsonElement payload)
            => ToActionResult(_service.ParseReferences(payload));

        [HttpGet("lookup/{label}/{fhirId}")]
        public async Task<IActionResult> LookupVertex(
            [FromRoute] string label,
            [FromRoute] string fhirId)
            => ToActionResult(await _service.LookupVertexAsync(label, "id", fhirId));
    }
}
