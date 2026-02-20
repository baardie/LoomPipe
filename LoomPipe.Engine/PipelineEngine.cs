using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Threading.Tasks;
using LoomPipe.Core.Entities;
using LoomPipe.Core.Exceptions;
using LoomPipe.Core.Interfaces;
using Microsoft.Extensions.Logging;

namespace LoomPipe.Engine
{
    /// <summary>
    /// Orchestrates pipeline execution: manages flow from source to destination,
    /// applies field mappings, transformations, and error handling.
    /// Each stage wraps failures with a <see cref="PipelineExecutionException"/> that
    /// identifies the failing stage and unwraps to the root-cause message.
    /// </summary>
    public class PipelineEngine
    {
        private readonly ISourceReader _sourceReader;
        private readonly IDestinationWriter _destinationWriter;
        private readonly ILogger<PipelineEngine> _logger;

        public PipelineEngine(ISourceReader sourceReader, IDestinationWriter destinationWriter, ILogger<PipelineEngine> logger)
        {
            _sourceReader        = sourceReader;
            _destinationWriter   = destinationWriter;
            _logger              = logger;
        }

        /// <summary>
        /// Runs the pipeline and returns the number of rows written to the destination.
        /// Supports batch writing when <see cref="Pipeline.BatchSize"/> is configured.
        /// </summary>
        public async Task<int> RunPipelineAsync(Pipeline pipeline)
        {
            _logger.LogInformation(
                "Pipeline '{PipelineName}' (Id={PipelineId}) started. Source={SourceType}, Destination={DestType}.",
                pipeline.Name, pipeline.Id,
                pipeline.Source?.Type ?? "?", pipeline.Destination?.Type ?? "?");

            // ── Stage 1: Read from source ─────────────────────────────────────
            IEnumerable<dynamic> sourceData;
            try
            {
                _logger.LogDebug(
                    "Stage [SourceRead] — reading from '{SourceName}' ({SourceType}).",
                    pipeline.Source?.Name, pipeline.Source?.Type);

                sourceData = await _sourceReader.ReadAsync(pipeline.Source);
            }
            catch (Exception ex) when (ex is not PipelineExecutionException)
            {
                var rootCause = GetRootCause(ex);
                _logger.LogError(ex,
                    "Pipeline '{PipelineName}' failed at [SourceRead]. Root cause: {RootCause}",
                    pipeline.Name, rootCause);

                throw new PipelineExecutionException(
                    $"Source read failed for '{pipeline.Source?.Name ?? pipeline.Source?.Type}': {rootCause}",
                    ex, "SourceRead");
            }

            // ── Stage 2: Map fields + apply transformations ───────────────────
            List<dynamic> transformedData;
            try
            {
                int mappingCount     = pipeline.FieldMappings?.Count ?? 0;
                int transformCount   = pipeline.Transformations?.Count ?? 0;
                _logger.LogDebug(
                    "Stage [Mapping] — applying {MappingCount} field map(s) and {TransformCount} transformation(s).",
                    mappingCount, transformCount);

                var mappedData   = ApplyFieldMappings(sourceData, pipeline.FieldMappings);
                transformedData  = ApplyTransformations(mappedData, pipeline.Transformations).ToList();
            }
            catch (Exception ex) when (ex is not PipelineExecutionException)
            {
                var rootCause = GetRootCause(ex);
                _logger.LogError(ex,
                    "Pipeline '{PipelineName}' failed at [Mapping]. Root cause: {RootCause}",
                    pipeline.Name, rootCause);

                throw new PipelineExecutionException(
                    $"Field mapping or transformation failed: {rootCause}",
                    ex, "Mapping");
            }

            int rowCount = transformedData.Count;
            _logger.LogInformation(
                "Pipeline '{PipelineName}': {RowCount} row(s) ready for destination.", pipeline.Name, rowCount);

            // ── Stage 3: Write to destination ─────────────────────────────────
            try
            {
                if (pipeline.BatchSize.HasValue && pipeline.BatchSize.Value > 0)
                {
                    await WriteBatchedAsync(pipeline, transformedData);
                }
                else
                {
                    _logger.LogDebug(
                        "Stage [DestinationWrite] — writing {RowCount} row(s) to '{DestName}' ({DestType}).",
                        rowCount, pipeline.Destination?.Name, pipeline.Destination?.Type);

                    await _destinationWriter.WriteAsync(pipeline.Destination, transformedData);
                }
            }
            catch (Exception ex) when (ex is not PipelineExecutionException)
            {
                var rootCause = GetRootCause(ex);
                _logger.LogError(ex,
                    "Pipeline '{PipelineName}' failed at [DestinationWrite]. Root cause: {RootCause}",
                    pipeline.Name, rootCause);

                throw new PipelineExecutionException(
                    $"Destination write failed for '{pipeline.Destination?.Name ?? pipeline.Destination?.Type}': {rootCause}",
                    ex, "DestinationWrite");
            }

            _logger.LogInformation(
                "Pipeline '{PipelineName}' (Id={PipelineId}) completed successfully. {RowCount} row(s) processed.",
                pipeline.Name, pipeline.Id, rowCount);

            return rowCount;
        }

        public async Task<DryRunResult> DryRunAsync(Pipeline pipeline, int sampleSize = 10)
        {
            _logger.LogInformation("Dry run started for '{PipelineName}'.", pipeline.Name);
            var sourceData = await _sourceReader.DryRunPreviewAsync(pipeline.Source, sampleSize);

            var sourcePreview      = sourceData.ToList();
            var mappedPreview      = ApplyFieldMappings(sourcePreview, pipeline.FieldMappings).ToList();
            var transformedPreview = ApplyTransformations(mappedPreview, pipeline.Transformations).ToList();

            _logger.LogInformation("Dry run for '{PipelineName}' completed successfully.", pipeline.Name);

            return new DryRunResult
            {
                SourcePreview      = sourcePreview.Take(sampleSize),
                MappedPreview      = mappedPreview.Take(sampleSize),
                TransformedPreview = transformedPreview.Take(sampleSize)
            };
        }

        // ── Private helpers ──────────────────────────────────────────────────

        private async Task WriteBatchedAsync(Pipeline pipeline, List<object> records)
        {
            int batchSize  = pipeline.BatchSize!.Value;
            int delayMs    = (pipeline.BatchDelaySeconds ?? 0) * 1000;
            int batchCount = (int)Math.Ceiling((double)records.Count / batchSize);

            _logger.LogInformation(
                "Stage [DestinationWrite/Batched] — writing {Total} row(s) across {Batches} batch(es) of {Size}.",
                records.Count, batchCount, batchSize);

            for (int i = 0; i < batchCount; i++)
            {
                var batch = records.Skip(i * batchSize).Take(batchSize).ToList();
                _logger.LogDebug("Batch {Batch}/{Total}: writing {Count} row(s).", i + 1, batchCount, batch.Count);
                await _destinationWriter.WriteAsync(pipeline.Destination, batch);

                if (delayMs > 0 && i < batchCount - 1)
                    await Task.Delay(delayMs);
            }
        }

        private IEnumerable<dynamic> ApplyFieldMappings(IEnumerable<dynamic> data, List<FieldMap> mappings)
        {
            if (mappings == null || !mappings.Any())
            {
                foreach (var record in data) yield return record;
                yield break;
            }

            foreach (var record in data)
            {
                IDictionary<string, object> mappedRecord = new ExpandoObject();
                var dict = (IDictionary<string, object>)record;
                foreach (var map in mappings)
                {
                    if (dict.ContainsKey(map.SourceField))
                        mappedRecord[map.DestinationField] = dict[map.SourceField];
                }
                yield return mappedRecord;
            }
        }

        private IEnumerable<dynamic> ApplyTransformations(IEnumerable<dynamic> data, List<string> transformations)
        {
            if (transformations == null || !transformations.Any())
            {
                foreach (var record in data) yield return record;
                yield break;
            }

            var transformationFuncs = transformations.Select(TransformationParser.Parse).ToList();

            foreach (var record in data)
            {
                var dict = (IDictionary<string, object>)record;
                foreach (var transform in transformationFuncs)
                {
                    transform(dict);
                }
                yield return record;
            }
        }

        /// <summary>
        /// Walks the exception chain and returns the innermost (root cause) message.
        /// </summary>
        private static string GetRootCause(Exception ex)
        {
            var current = ex;
            while (current.InnerException != null)
                current = current.InnerException;
            return current.Message;
        }
    }
}
