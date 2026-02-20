using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace LoomPipe.Server.Controllers
{
    /// <summary>
    /// Handles JSON file uploads for use as pipeline sources.
    /// Uploaded files are stored in the <c>json-uploads/</c> directory and
    /// the full server path is returned for use in the pipeline source config.
    /// </summary>
    [ApiController]
    [Route("api/json")]
    [Authorize]
    public class JsonController : ControllerBase
    {
        private readonly IWebHostEnvironment _env;
        private readonly ILogger<JsonController> _logger;

        public JsonController(IWebHostEnvironment env, ILogger<JsonController> logger)
        {
            _env    = env;
            _logger = logger;
        }

        [HttpPost("upload")]
        [RequestSizeLimit(104_857_600)] // 100 MB — JSON can be larger than CSV
        public async Task<IActionResult> Upload(IFormFile file)
        {
            if (file == null || file.Length == 0)
                return BadRequest(new { error = "No file provided." });

            var ext = Path.GetExtension(file.FileName).ToLowerInvariant();
            if (ext != ".json")
                return BadRequest(new { error = "Only .json files are accepted." });

            // Basic JSON validity check before saving
            try
            {
                using var stream = file.OpenReadStream();
                using var doc    = await System.Text.Json.JsonDocument.ParseAsync(stream);
                var kind = doc.RootElement.ValueKind;
                if (kind != System.Text.Json.JsonValueKind.Array && kind != System.Text.Json.JsonValueKind.Object)
                    return BadRequest(new { error = "JSON root must be an array [...] or an object {...}." });
            }
            catch (System.Text.Json.JsonException jex)
            {
                return BadRequest(new { error = $"Invalid JSON: {jex.Message}" });
            }

            var uploadDir = Path.Combine(_env.ContentRootPath, "json-uploads");
            Directory.CreateDirectory(uploadDir);

            var safeName = $"{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}_{Path.GetFileName(file.FileName)}";
            var fullPath = Path.Combine(uploadDir, safeName);

            await using var fs = System.IO.File.Create(fullPath);
            file.OpenReadStream().Position = 0;
            await file.CopyToAsync(fs);

            _logger.LogInformation("JSON file uploaded: {Path} ({Bytes} bytes)", fullPath, file.Length);

            return Ok(new { path = fullPath, fileName = file.FileName, sizeBytes = file.Length });
        }
    }
}
