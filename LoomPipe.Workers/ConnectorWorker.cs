using LoomPipe.Core.Entities;
using LoomPipe.Core.Enums;
using LoomPipe.Core.Interfaces;
using LoomPipe.Engine;
using LoomPipe.Storage.Interfaces;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace LoomPipe.Workers
{
    public class ConnectorWorker : BackgroundService
    {
        private readonly IServiceScopeFactory _scopeFactory;
        private readonly ILogger<ConnectorWorker> _logger;
        private readonly ConcurrentDictionary<int, IConnector> _running = new();
        private DateTime _lastScheduleCheck = DateTime.MinValue;

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

                await Task.Delay(1000, stoppingToken);
            }
        }

        private async Task CheckScheduledPipelinesAsync(IServiceProvider services, CancellationToken stoppingToken)
        {
            try
            {
                var pipelineRepo   = services.GetRequiredService<IPipelineRepository>();
                var runLogRepo     = services.GetRequiredService<IPipelineRunLogRepository>();
                var connectorFactory = services.GetRequiredService<IConnectorFactory>();
                var profileService = services.GetRequiredService<IConnectionProfileService>();
                var engineLogger   = services.GetRequiredService<ILogger<PipelineEngine>>();

                var now       = DateTime.UtcNow;
                var pipelines = await pipelineRepo.GetAllAsync();
                var due       = pipelines
                    .Where(p => p.ScheduleEnabled && p.NextRunAt.HasValue && p.NextRunAt.Value <= now)
                    .ToList();

                foreach (var pipeline in due)
                {
                    if (stoppingToken.IsCancellationRequested) break;
                    _logger.LogInformation("Scheduler triggering pipeline '{Name}' (id={Id}).", pipeline.Name, pipeline.Id);

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

                    var log = new PipelineRunLog
                    {
                        PipelineId  = pipeline.Id,
                        StartedAt   = DateTime.UtcNow,
                        Status      = "Running",
                        TriggeredBy = "scheduler",
                    };
                    await runLogRepo.AddAsync(log);

                    try
                    {
                        var engine = new PipelineEngine(sourceReader, destinationWriter, engineLogger);
                        var rows   = await engine.RunPipelineAsync(pipeline);

                        log.FinishedAt    = DateTime.UtcNow;
                        log.Status        = "Success";
                        log.RowsProcessed = rows;
                        await runLogRepo.UpdateAsync(log);
                        _logger.LogInformation("Scheduled pipeline '{Name}' completed ({Rows} rows).", pipeline.Name, rows);
                    }
                    catch (Exception ex)
                    {
                        log.FinishedAt   = DateTime.UtcNow;
                        log.Status       = "Failed";
                        log.ErrorMessage = ex.Message;
                        await runLogRepo.UpdateAsync(log);
                        _logger.LogError(ex, "Scheduled pipeline '{Name}' failed.", pipeline.Name);
                    }

                    // Advance NextRunAt
                    if (pipeline.ScheduleIntervalMinutes.HasValue && pipeline.ScheduleIntervalMinutes.Value > 0)
                    {
                        pipeline.NextRunAt = now.AddMinutes(pipeline.ScheduleIntervalMinutes.Value);
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
