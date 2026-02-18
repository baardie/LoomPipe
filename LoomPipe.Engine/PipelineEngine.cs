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
    /// Orchestrates pipeline execution: manages flow from source to destination, applies field mappings, transformations, and error handling.
    /// Returns the number of rows processed.
    /// </summary>
    public class PipelineEngine
    {
        private readonly ISourceReader _sourceReader;
        private readonly IDestinationWriter _destinationWriter;
        private readonly ILogger<PipelineEngine> _logger;

        public PipelineEngine(ISourceReader sourceReader, IDestinationWriter destinationWriter, ILogger<PipelineEngine> logger)
        {
            _sourceReader = sourceReader;
            _destinationWriter = destinationWriter;
            _logger = logger;
        }

        /// <summary>
        /// Runs the pipeline and returns the number of rows written to the destination.
        /// Supports batch writing when <see cref="Pipeline.BatchSize"/> is configured.
        /// </summary>
        public async Task<int> RunPipelineAsync(Pipeline pipeline)
        {
            _logger.LogInformation("Pipeline run started for '{PipelineName}'.", pipeline.Name);
            try
            {
                _logger.LogDebug("Reading data from source '{SourceName}'.", pipeline.Source.Name);
                var sourceData = await _sourceReader.ReadAsync(pipeline.Source);

                _logger.LogDebug("Applying field mappings.");
                var mappedData = ApplyFieldMappings(sourceData, pipeline.FieldMappings);

                _logger.LogDebug("Applying transformations.");
                var transformedData = ApplyTransformations(mappedData, pipeline.Transformations).ToList();

                int rowCount = transformedData.Count;

                if (pipeline.BatchSize.HasValue && pipeline.BatchSize.Value > 0)
                {
                    await WriteBatchedAsync(pipeline, transformedData);
                }
                else
                {
                    _logger.LogDebug("Writing data to destination '{DestinationName}'.", pipeline.Destination.Name);
                    await _destinationWriter.WriteAsync(pipeline.Destination, transformedData);
                }

                _logger.LogInformation("Pipeline run for '{PipelineName}' completed successfully ({Rows} rows).", pipeline.Name, rowCount);
                return rowCount;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Pipeline execution failed for '{PipelineName}'.", pipeline.Name);
                throw new PipelineExecutionException($"Pipeline execution failed for '{pipeline.Name}'. See inner exception for details.", ex);
            }
        }

        public async Task<DryRunResult> DryRunAsync(Pipeline pipeline, int sampleSize = 10)
        {
            _logger.LogInformation("Dry run started for '{PipelineName}'.", pipeline.Name);
            var sourceData = await _sourceReader.DryRunPreviewAsync(pipeline.Source, sampleSize);

            var sourcePreview = sourceData.ToList();
            var mappedPreview = ApplyFieldMappings(sourcePreview, pipeline.FieldMappings).ToList();
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

            _logger.LogInformation("Batch writing {Total} rows in {Batches} batches of {Size}.", records.Count, batchCount, batchSize);

            for (int i = 0; i < batchCount; i++)
            {
                var batch = records.Skip(i * batchSize).Take(batchSize).ToList();
                _logger.LogDebug("Writing batch {Batch}/{Total} ({Count} rows).", i + 1, batchCount, batch.Count);
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
    }
}
