using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Claims;
using System.Threading.Tasks;
using LoomPipe.Core.DTOs;
using LoomPipe.Core.Entities;
using LoomPipe.Core.Exceptions;
using LoomPipe.Core.Interfaces;
using LoomPipe.Engine;
using LoomPipe.Storage.Interfaces;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;

namespace LoomPipe.Server.Controllers
{
    public class AutomapRequest
    {
        public DataSourceConfig Source { get; set; } = new();
        public DataSourceConfig Destination { get; set; } = new();
    }

    [ApiController]
    [Route("api/[controller]")]
    [Authorize]
    public class PipelinesController : ControllerBase
    {
        private readonly IPipelineRepository _pipelineRepository;
        private readonly IConnectorFactory _connectorFactory;
        private readonly IConnectionProfileService _connectionProfileService;
        private readonly IPipelineRunLogRepository _runLogRepo;
        private readonly IEmailNotificationService _emailNotificationService;
        private readonly INotificationRepository _notifRepo;
        private readonly ILogger<PipelinesController> _logger;

        public PipelinesController(
            IPipelineRepository pipelineRepository,
            IConnectorFactory connectorFactory,
            IConnectionProfileService connectionProfileService,
            IPipelineRunLogRepository runLogRepo,
            IEmailNotificationService emailNotificationService,
            INotificationRepository notifRepo,
            ILogger<PipelinesController> logger)
        {
            _pipelineRepository       = pipelineRepository;
            _connectorFactory         = connectorFactory;
            _connectionProfileService = connectionProfileService;
            _runLogRepo               = runLogRepo;
            _emailNotificationService = emailNotificationService;
            _notifRepo                = notifRepo;
            _logger                   = logger;
        }

        /// <summary>
        /// Returns all pipelines enriched with last-run status so the UI can show
        /// a status badge on each row without a separate request per pipeline.
        /// </summary>
        [HttpGet]
        public async Task<IActionResult> Get()
        {
            var pipelines  = await _pipelineRepository.GetAllAsync();
            var latestRuns = await _runLogRepo.GetLatestRunPerPipelineAsync();

            var result = pipelines.Select(p =>
            {
                latestRuns.TryGetValue(p.Id, out var last);
                return new
                {
                    p.Id, p.Name, p.Source, p.Destination,
                    p.FieldMappings, p.Transformations,
                    p.ScheduleEnabled, p.CronExpression, p.NextRunAt,
                    p.BatchSize, p.BatchDelaySeconds,
                    p.IncrementalField, p.LastIncrementalValue,
                    p.CreatedAt,
                    LastRunStatus    = last?.Status,
                    LastRunAt        = last?.StartedAt,
                    LastErrorMessage = last?.ErrorMessage,
                };
            });

            return Ok(result);
        }

        [HttpGet("{id}")]
        public async Task<ActionResult<Pipeline>> Get(int id)
        {
            var pipeline = await _pipelineRepository.GetByIdAsync(id);
            if (pipeline == null) return NotFound();
            return pipeline;
        }

        [HttpPost]
        [Authorize(Roles = "Admin")]
        public async Task<ActionResult<Pipeline>> Post([FromBody] Pipeline pipeline)
        {
            pipeline.CreatedAt = DateTime.UtcNow;
            await _pipelineRepository.AddAsync(pipeline);
            return CreatedAtAction(nameof(Get), new { id = pipeline.Id }, pipeline);
        }

        [HttpPut("{id}")]
        [Authorize(Roles = "Admin")]
        public async Task<IActionResult> Put(int id, [FromBody] Pipeline pipeline)
        {
            if (id != pipeline.Id) return BadRequest();
            await _pipelineRepository.UpdateAsync(pipeline);
            return NoContent();
        }

        [HttpDelete("{id}")]
        [Authorize(Roles = "Admin")]
        public async Task<IActionResult> Delete(int id)
        {
            await _pipelineRepository.DeleteAsync(id);
            return NoContent();
        }

        [HttpPost("{id}/run")]
        [Authorize(Roles = "Admin,User")]
        public async Task<IActionResult> Run(int id)
        {
            var pipeline = await _pipelineRepository.GetByIdAsync(id);
            if (pipeline == null) return NotFound();

            await ResolveProfileAsync(pipeline.Source);
            await ResolveProfileAsync(pipeline.Destination);

            ISourceReader sourceReader;
            IDestinationWriter destinationWriter;
            try
            {
                sourceReader      = _connectorFactory.CreateSourceReader(pipeline.Source.Type);
                destinationWriter = _connectorFactory.CreateDestinationWriter(pipeline.Destination.Type);
            }
            catch (System.NotSupportedException ex)
            {
                return BadRequest(ex.Message);
            }

            var triggeredBy  = User.FindFirstValue(ClaimTypes.Name) ?? "system";
            var runStartTime = DateTime.UtcNow;
            var log = new PipelineRunLog
            {
                PipelineId  = pipeline.Id,
                StartedAt   = runStartTime,
                Status      = "Running",
                TriggeredBy = triggeredBy,
            };
            await _runLogRepo.AddAsync(log);

            try
            {
                var engine = new PipelineEngine(sourceReader, destinationWriter,
                    HttpContext.RequestServices.GetRequiredService<ILogger<PipelineEngine>>());

                var rows = await engine.RunPipelineAsync(pipeline);

                log.FinishedAt    = DateTime.UtcNow;
                log.Status        = "Success";
                log.RowsProcessed = rows;
                await _runLogRepo.UpdateAsync(log);

                // Advance incremental watermark
                if (!string.IsNullOrWhiteSpace(pipeline.IncrementalField))
                {
                    pipeline.LastIncrementalValue = runStartTime.ToString("o");
                    await _pipelineRepository.UpdateAsync(pipeline);
                }

                _ = NotifySuccessAsync(pipeline, rows, triggeredBy, log.FinishedAt.Value);
                _ = _notifRepo.AddAsync(new Notification
                {
                    Type       = "pipeline.success",
                    Title      = $"{pipeline.Name} completed",
                    Message    = $"{rows:N0} rows processed in {(log.FinishedAt.Value - log.StartedAt).TotalSeconds:F1}s",
                    PipelineId = pipeline.Id,
                    CreatedAt  = DateTime.UtcNow,
                });

                return Ok(new { rowsProcessed = rows });
            }
            catch (Exception ex)
            {
                var (userMessage, stage) = ExtractError(ex);

                _logger.LogError(ex,
                    "Pipeline {PipelineName} (Id={PipelineId}) failed at stage {Stage} triggered by {TriggeredBy}.",
                    pipeline.Name, pipeline.Id, stage ?? "unknown", triggeredBy);

                log.FinishedAt   = DateTime.UtcNow;
                log.Status       = "Failed";
                log.ErrorMessage = userMessage;
                await _runLogRepo.UpdateAsync(log);

                _ = NotifyFailureAsync(pipeline, userMessage, stage ?? "Pipeline", triggeredBy, log.FinishedAt.Value);
                _ = _notifRepo.AddAsync(new Notification
                {
                    Type       = "pipeline.failed",
                    Title      = $"{pipeline.Name} failed",
                    Message    = userMessage,
                    PipelineId = pipeline.Id,
                    CreatedAt  = DateTime.UtcNow,
                });

                return StatusCode(500, new
                {
                    message    = userMessage,
                    stage      = stage,
                    pipelineId = pipeline.Id,
                });
            }
        }

        [HttpGet("{id}/runs")]
        public async Task<IActionResult> GetRuns(int id, [FromQuery] int limit = 10)
        {
            var logs = await _runLogRepo.GetByPipelineIdAsync(id, limit);
            var dtos = logs.Select(r => new PipelineRunLogDto
            {
                Id            = r.Id,
                PipelineId    = r.PipelineId,
                StartedAt     = r.StartedAt,
                FinishedAt    = r.FinishedAt,
                DurationMs    = r.FinishedAt.HasValue
                                    ? (long)(r.FinishedAt.Value - r.StartedAt).TotalMilliseconds
                                    : null,
                Status        = r.Status,
                RowsProcessed = r.RowsProcessed,
                ErrorMessage  = r.ErrorMessage,
                TriggeredBy   = r.TriggeredBy,
            });
            return Ok(dtos);
        }

        [HttpGet("{id}/stats")]
        public async Task<IActionResult> GetStats(int id)
        {
            var stats = await _runLogRepo.GetStatsAsync(id);
            return Ok(stats);
        }

        [HttpPost("automap")]
        public async Task<ActionResult<IEnumerable<FieldMap>>> Automap([FromBody] AutomapRequest request)
        {
            if (request?.Source == null || request.Destination == null)
                return BadRequest("Invalid automap request.");

            await ResolveProfileAsync(request.Source);

            ISourceReader sourceReader;
            try
            {
                sourceReader = _connectorFactory.CreateSourceReader(request.Source.Type);
            }
            catch (System.NotSupportedException ex)
            {
                return BadRequest(ex.Message);
            }

            var sourceSchema      = await sourceReader.DiscoverSchemaAsync(request.Source);
            var destinationSchema = request.Destination.Schema?.Split(',').Select(s => s.Trim())
                                    ?? Enumerable.Empty<string>();
            var mappings          = AutomapHelper.AutomapFields(sourceSchema, destinationSchema);

            var fieldMaps = mappings.Select(m => new FieldMap
            {
                SourceField      = m.source,
                DestinationField = m.dest,
                AutomapScore     = m.score,
                IsAutomapped     = true
            }).ToList();

            return Ok(fieldMaps);
        }

        [HttpPost("dryrun")]
        public async Task<ActionResult<DryRunResult>> DryRun([FromBody] Pipeline pipeline)
        {
            if (pipeline == null) return BadRequest("Invalid pipeline.");

            await ResolveProfileAsync(pipeline.Source);
            await ResolveProfileAsync(pipeline.Destination);

            ISourceReader sourceReader;
            IDestinationWriter destinationWriter;
            try
            {
                sourceReader      = _connectorFactory.CreateSourceReader(pipeline.Source.Type);
                destinationWriter = _connectorFactory.CreateDestinationWriter(pipeline.Destination.Type);
            }
            catch (System.NotSupportedException ex)
            {
                return BadRequest(ex.Message);
            }

            var engine = new PipelineEngine(sourceReader, destinationWriter,
                HttpContext.RequestServices.GetRequiredService<ILogger<PipelineEngine>>());
            var result = await engine.DryRunAsync(pipeline);
            return Ok(result);
        }

        // ── Private helpers ───────────────────────────────────────────────────

        private async Task ResolveProfileAsync(DataSourceConfig config)
        {
            if (config?.Parameters == null) return;
            if (config.Parameters.TryGetValue("connectionProfileId", out var raw)
                && int.TryParse(raw?.ToString(), out var profileId))
            {
                config.ConnectionString = await _connectionProfileService.BuildConnectionStringAsync(profileId);
            }
        }

        /// <summary>
        /// Extracts a clean, human-readable error message and failing stage from the
        /// exception chain. Eliminates generic "See inner exception..." noise.
        /// </summary>
        private static (string message, string? stage) ExtractError(Exception ex)
        {
            if (ex is PipelineExecutionException pex)
                return (pex.GetDetailedMessage(), pex.Stage);

            // Unexpected exception — walk to root cause for the most useful message
            var root = ex;
            while (root.InnerException != null)
                root = root.InnerException;

            return (root.Message, null);
        }

        private async Task NotifyFailureAsync(Pipeline pipeline, string errorMessage, string stage, string triggeredBy, DateTime failedAt)
        {
            try
            {
                await _emailNotificationService.SendPipelineFailureAsync(
                    pipeline.Name, pipeline.Id, errorMessage, stage, triggeredBy, failedAt);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failure notification could not be delivered for pipeline {Name}.", pipeline.Name);
            }
        }

        private async Task NotifySuccessAsync(Pipeline pipeline, int rows, string triggeredBy, DateTime completedAt)
        {
            try
            {
                await _emailNotificationService.SendPipelineSuccessAsync(
                    pipeline.Name, pipeline.Id, rows, triggeredBy, completedAt);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Success notification could not be delivered for pipeline {Name}.", pipeline.Name);
            }
        }
    }
}
