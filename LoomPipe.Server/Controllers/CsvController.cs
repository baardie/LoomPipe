using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace LoomPipe.Server.Controllers
{
    [ApiController]
    [Route("api/csv")]
    [Authorize]
    public class CsvController : ControllerBase
    {
        private readonly IWebHostEnvironment _env;
        private readonly ILogger<CsvController> _logger;

        public CsvController(IWebHostEnvironment env, ILogger<CsvController> logger)
        {
            _env = env;
            _logger = logger;
        }

        [HttpPost("upload")]
        [RequestSizeLimit(52_428_800)] // 50 MB
        public async Task<IActionResult> Upload(IFormFile file)
        {
            if (file == null || file.Length == 0)
                return BadRequest(new { error = "No file provided." });

            var ext = Path.GetExtension(file.FileName).ToLowerInvariant();
            if (ext != ".csv")
                return BadRequest(new { error = "Only .csv files are accepted." });

            var uploadDir = Path.Combine(_env.ContentRootPath, "csv-uploads");
            Directory.CreateDirectory(uploadDir);

            // Prefix with timestamp to avoid collisions
            var safeName = $"{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}_{Path.GetFileName(file.FileName)}";
            var fullPath = Path.Combine(uploadDir, safeName);

            await using var stream = System.IO.File.Create(fullPath);
            await file.CopyToAsync(stream);

            _logger.LogInformation("CSV uploaded: {Path} ({Bytes} bytes)", fullPath, file.Length);

            return Ok(new { path = fullPath, fileName = file.FileName, sizeBytes = file.Length });
        }
    }
}
