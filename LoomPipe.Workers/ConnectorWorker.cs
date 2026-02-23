using Cronos;
using LoomPipe.Core.Entities;
using LoomPipe.Core.Enums;
using LoomPipe.Core.Exceptions;
using LoomPipe.Core.Interfaces;
using LoomPipe.Engine;
using LoomPipe.Storage.Interfaces;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;

namespace LoomPipe.Workers
{
    public class ConnectorWorker : BackgroundService
    {
        private static readonly JsonSerializerOptions _snapshotOpts = new()
        {
            ReferenceHandler = ReferenceHandler.IgnoreCycles,
        };

        private readonly IServiceScopeFactory _scopeFactory;
        private readonly ILogger<ConnectorWorker> _logger;
        private readonly ConcurrentDictionary<int, IConnector> _running = new();
        private DateTime _lastScheduleCheck  = DateTime.MinValue;
        private DateTime _lastSnapshotCleanup = DateTime.MinValue;

        public ConnectorWorker(IServiceScopeFactory scopeFactory, ILogger<ConnectorWorker> logger)
        {
            _scopeFactory = scopeFactory;
            _logger = logger;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                await using var scope = _scopeFactory.CreateAsyncScope();
                var connectorService = scope.ServiceProvider.GetRequiredService<IConnectorService>();
                var connectors = await connectorService.GetConnectorsAsync();

                foreach (var connector in connectors)
                {
                    if (connector.Status == ConnectorStatus.Running && !_running.ContainsKey(connector.Id))
                    {
                        await StartConnector(connector, scope.ServiceProvider, stoppingToken);
                    }
                    else if (connector.Status != ConnectorStatus.Running && _running.TryRemove(connector.Id, out var instance))
                    {
                        await StopConnector(connector, instance, stoppingToken);
                    }
                }

                // Check scheduled pipelines every 60 seconds
                if ((DateTime.UtcNow - _lastScheduleCheck).TotalSeconds >= 60)
                {
                    _lastScheduleCheck = DateTime.UtcNow;
                    await CheckScheduledPipelinesAsync(scope.ServiceProvider, stoppingToken);
                }

                // Purge expired config snapshots once per day
                if ((DateTime.UtcNow - _lastSnapshotCleanup).TotalHours >= 24)
                {
                    _lastSnapshotCleanup = DateTime.UtcNow;
                    await PurgeExpiredSnapshotsAsync(scope.ServiceProvider);
                }

                await Task.Delay(1000, stoppingToken);
            }
        }

        private async Task PurgeExpiredSnapshotsAsync(IServiceProvider services)
        {
            try
            {
                var db  = services.GetRequiredService<LoomPipe.Storage.LoomPipeDbContext>();
                var now = DateTime.UtcNow;
                var count = await db.PipelineRunLogs
                    .Where(r => r.SnapshotExpiresAt.HasValue && r.SnapshotExpiresAt.Value <= now
                                && r.ConfigSnapshot != null)
                    .ExecuteUpdateAsync(s => s
                        .SetProperty(r => r.ConfigSnapshot,    (string?)null)
                        .SetProperty(r => r.SnapshotExpiresAt, (DateTime?)null));

                if (count > 0)
                    _logger.LogInformation("Purged {Count} expired run config snapshot(s).", count);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Error purging expired config snapshots.");
            }
        }

        private async Task CheckScheduledPipelinesAsync(IServiceProvider services, CancellationToken stoppingToken)
        {
            try
            {
                var pipelineRepo     = services.GetRequiredService<IPipelineRepository>();
                var runLogRepo       = services.GetRequiredService<IPipelineRunLogRepository>();
                var connectorFactory = services.GetRequiredService<IConnectorFactory>();
                var profileService   = services.GetRequiredService<IConnectionProfileService>();
                var engineLogger     = services.GetRequiredService<ILogger<PipelineEngine>>();
                var notifRepo        = services.GetRequiredService<INotificationRepository>();
                var systemSettings   = services.GetRequiredService<ISystemSettingsRepository>();

                var settings   = await systemSettings.GetAsync();
                var retainDays = Math.Max(1, settings.FailedRunRetentionDays);

                var now       = DateTime.UtcNow;
                var pipelines = await pipelineRepo.GetAllAsync();
                var due       = pipelines
                    .Where(p => p.ScheduleEnabled && p.NextRunAt.HasValue && p.NextRunAt.Value <= now)
                    .ToList();

                foreach (var pipeline in due)
                {
                    if (stoppingToken.IsCancellationRequested) break;
                    _logger.LogInformation("Scheduler triggering pipeline '{Name}' (id={Id}).", pipeline.Name, pipeline.Id);

                    // Capture config snapshot BEFORE resolving profiles
                    string? snapshot   = null;
                    DateTime? expiry   = null;
                    try
                    {
                        snapshot = JsonSerializer.Serialize(pipeline, _snapshotOpts);
                        expiry   = now.AddDays(retainDays);
                    }
                    catch (Exception snapshotEx)
                    {
                        _logger.LogWarning(snapshotEx, "Could not serialize config snapshot for scheduled pipeline '{Name}'.", pipeline.Name);
                    }

                    await ResolveProfileAsync(pipeline.Source, profileService);
                    await ResolveProfileAsync(pipeline.Destination, profileService);

                    ISourceReader sourceReader;
                    IDestinationWriter destinationWriter;
                    try
                    {
                        sourceReader      = connectorFactory.CreateSourceReader(pipeline.Source.Type);
                        destinationWriter = connectorFactory.CreateDestinationWriter(pipeline.Destination.Type);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Cannot create connectors for scheduled pipeline '{Name}'.", pipeline.Name);
                        continue;
                    }

                    var runStartTime = now;
                    var log = new PipelineRunLog
                    {
                        PipelineId        = pipeline.Id,
                        StartedAt         = runStartTime,
                        Status            = "Running",
                        TriggeredBy       = "scheduler",
                        ConfigSnapshot    = snapshot,
                        SnapshotExpiresAt = expiry,
                    };
                    await runLogRepo.AddAsync(log);

                    var emailService = services.GetRequiredService<IEmailNotificationService>();

                    try
                    {
                        var engine = new PipelineEngine(sourceReader, destinationWriter, engineLogger);
                        var rows   = await engine.RunPipelineAsync(pipeline);

                        log.FinishedAt    = DateTime.UtcNow;
                        log.Status        = "Success";
                        log.RowsProcessed = rows;
                        await runLogRepo.UpdateAsync(log);
                        _logger.LogInformation("Scheduled pipeline '{Name}' completed ({Rows} rows).", pipeline.Name, rows);

                        if (!string.IsNullOrWhiteSpace(pipeline.IncrementalField))
                            pipeline.LastIncrementalValue = runStartTime.ToString("o");

                        try
                        {
                            await emailService.SendPipelineSuccessAsync(
                                pipeline.Name, pipeline.Id, rows, "scheduler", log.FinishedAt.Value);
                        }
                        catch (Exception mailEx)
                        {
                            _logger.LogWarning(mailEx, "Success email could not be delivered for scheduled pipeline '{Name}'.", pipeline.Name);
                        }

                        try
                        {
                            await notifRepo.AddAsync(new Notification
                            {
                                Type       = "pipeline.success",
                                Title      = $"{pipeline.Name} completed",
                                Message    = $"{rows:N0} rows processed in {(log.FinishedAt!.Value - log.StartedAt).TotalSeconds:F1}s (scheduled)",
                                PipelineId = pipeline.Id,
                                CreatedAt  = DateTime.UtcNow,
                            });
                        }
                        catch (Exception notifEx)
                        {
                            _logger.LogWarning(notifEx, "In-app notification could not be created for scheduled pipeline '{Name}'.", pipeline.Name);
                        }
                    }
                    catch (Exception ex)
                    {
                        var (errorMessage, stage) = ex is PipelineExecutionException pex
                            ? (pex.GetDetailedMessage(), pex.Stage)
                            : (ex.Message, null);

                        log.FinishedAt   = DateTime.UtcNow;
                        log.Status       = "Failed";
                        log.ErrorMessage = errorMessage;
                        await runLogRepo.UpdateAsync(log);

                        _logger.LogError(ex,
                            "Scheduled pipeline '{Name}' (Id={Id}) failed at stage '{Stage}'.",
                            pipeline.Name, pipeline.Id, stage ?? "unknown");

                        try
                        {
                            await emailService.SendPipelineFailureAsync(
                                pipeline.Name, pipeline.Id, errorMessage,
                                stage ?? "Pipeline", "scheduler", log.FinishedAt.Value);
                        }
                        catch (Exception mailEx)
                        {
                            _logger.LogWarning(mailEx, "Failure email could not be delivered for scheduled pipeline '{Name}'.", pipeline.Name);
                        }

                        try
                        {
                            await notifRepo.AddAsync(new Notification
                            {
                                Type       = "pipeline.failed",
                                Title      = $"{pipeline.Name} failed",
                                Message    = errorMessage,
                                PipelineId = pipeline.Id,
                                CreatedAt  = DateTime.UtcNow,
                            });
                        }
                        catch (Exception notifEx)
                        {
                            _logger.LogWarning(notifEx, "In-app notification could not be created for failed scheduled pipeline '{Name}'.", pipeline.Name);
                        }
                    }

                    // Advance NextRunAt using the cron expression
                    if (!string.IsNullOrWhiteSpace(pipeline.CronExpression))
                    {
                        try
                        {
                            var cron = CronExpression.Parse(pipeline.CronExpression, CronFormat.Standard);
                            pipeline.NextRunAt = cron.GetNextOccurrence(now, TimeZoneInfo.Utc);
                        }
                        catch (CronFormatException ex)
                        {
                            _logger.LogError(ex, "Invalid cron expression '{Expr}' on pipeline '{Name}'; schedule disabled.",
                                pipeline.CronExpression, pipeline.Name);
                            pipeline.ScheduleEnabled = false;
                        }
                        await pipelineRepo.UpdateAsync(pipeline);
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error during scheduled pipeline check.");
            }
        }

        private static async Task ResolveProfileAsync(DataSourceConfig config, IConnectionProfileService profileService)
        {
            if (config?.Parameters == null) return;
            if (config.Parameters.TryGetValue("connectionProfileId", out var raw)
                && int.TryParse(raw?.ToString(), out var profileId))
            {
                config.ConnectionString = await profileService.BuildConnectionStringAsync(profileId);
            }
        }

        private async Task StartConnector(Connector connector, IServiceProvider serviceProvider, CancellationToken cancellationToken)
        {
            if (string.IsNullOrWhiteSpace(connector.AssemblyQualifiedName))
            {
                _logger.LogWarning("Connector {Id} has no AssemblyQualifiedName; skipping start.", connector.Id);
                return;
            }

            var type = System.Type.GetType(connector.AssemblyQualifiedName);
            if (type == null)
            {
                _logger.LogError("Could not resolve type '{Type}' for connector {Id}.", connector.AssemblyQualifiedName, connector.Id);
                return;
            }

            try
            {
                var instance = (IConnector)ActivatorUtilities.CreateInstance(serviceProvider, type);
                await instance.StartAsync(cancellationToken);
                _running[connector.Id] = instance;
                _logger.LogInformation("Connector {Id} ({Name}) started.", connector.Id, connector.Name);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to start connector {Id} ({Name}).", connector.Id, connector.Name);
            }
        }

        private async Task StopConnector(Connector connector, IConnector instance, CancellationToken cancellationToken)
        {
            try
            {
                await instance.StopAsync(cancellationToken);
                _logger.LogInformation("Connector {Id} ({Name}) stopped.", connector.Id, connector.Name);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to stop connector {Id} ({Name}).", connector.Id, connector.Name);
            }
        }
    }
}
