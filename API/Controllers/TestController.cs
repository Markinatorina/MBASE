using Microsoft.AspNetCore.Mvc;
using DAL.Repositories;

namespace API.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class TestController : ControllerBase
    {
        private readonly IGraphRepository _repo;

        public TestController(IGraphRepository repo)
        {
            _repo = repo;
        }

        // GET api/test/ping -> quick connectivity + vertex count
        [HttpGet("ping")]
        public async Task<ActionResult<object>> Ping()
        {
            var count = await _repo.CountVerticesAsync();
            return Ok(new { status = "ok", vertexCount = count });
        }
        
        // POST api/test/vertex/{label}
        // body: { "key": "value", ... }
        [HttpPost("vertex/{label}")]
        public async Task<ActionResult<object>> CreateVertex([FromRoute] string label, [FromBody] Dictionary<string, object>? properties)
        {
            var props = properties ?? new Dictionary<string, object>();
            var v = await _repo.AddVertexAsync(label, props);
            return Ok(new { id = v.Id?.ToString(), label = v.Label, properties = v.Properties });
        }

        // GET api/test/vertex/{id}
        [HttpGet("vertex/{id}")]
        public async Task<ActionResult<object>> GetVertex([FromRoute] string id)
        {
            var v = await _repo.GetVertexByIdAsync(id);
            if (v == null) return NotFound();
            return Ok(new { id = v.Id?.ToString(), label = v.Label, properties = v.Properties });
        }

        // PATCH api/test/vertex/{id}
        // body: { "key": "value", ... }
        [HttpPatch("vertex/{id}")]
        public async Task<IActionResult> UpdateVertex([FromRoute] string id, [FromBody] Dictionary<string, object> properties)
        {
            if (properties == null || properties.Count == 0) return BadRequest("No properties provided");
            var updated = await _repo.UpdateVertexPropertiesAsync(id, properties);
            return updated ? NoContent() : NotFound();
        }

        // DELETE api/test/vertex/{id}
        [HttpDelete("vertex/{id}")]
        public async Task<IActionResult> DeleteVertex([FromRoute] string id)
        {
            var _ = await _repo.DeleteVertexAsync(id);
            return NoContent();
        }
    }
}
