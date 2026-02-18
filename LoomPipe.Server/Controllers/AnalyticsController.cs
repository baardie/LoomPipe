using System.Threading.Tasks;
using LoomPipe.Core.Interfaces;
using LoomPipe.Storage.Interfaces;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace LoomPipe.Server.Controllers
{
    [ApiController]
    [Route("api/analytics")]
    [Authorize]
    public class AnalyticsController : ControllerBase
    {
        private readonly IPipelineRunLogRepository _runLogs;
        private readonly IPipelineRepository _pipelines;

        public AnalyticsController(IPipelineRunLogRepository runLogs, IPipelineRepository pipelines)
        {
            _runLogs   = runLogs;
            _pipelines = pipelines;
        }

        [HttpGet("summary")]
        public async Task<IActionResult> Summary()
        {
            var allPipelines = await _pipelines.GetAllAsync();
            int count = 0;
            foreach (var _ in allPipelines) count++;
            var summary = await _runLogs.GetSummaryAsync(count);
            return Ok(summary);
        }

        [HttpGet("runs-by-day")]
        public async Task<IActionResult> RunsByDay([FromQuery] int days = 7)
        {
            if (days < 1) days = 1;
            if (days > 90) days = 90;
            var result = await _runLogs.GetRunsByDayAsync(days);
            return Ok(result);
        }
    }
}
