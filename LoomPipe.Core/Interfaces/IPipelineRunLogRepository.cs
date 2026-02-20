using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using LoomPipe.Core.DTOs;
using LoomPipe.Core.Entities;

namespace LoomPipe.Core.Interfaces
{
    public interface IPipelineRunLogRepository
    {
        Task AddAsync(PipelineRunLog log);
        Task UpdateAsync(PipelineRunLog log);
        Task<PipelineRunLog?> GetByIdAsync(int id);
        Task<IEnumerable<PipelineRunLog>> GetByPipelineIdAsync(int pipelineId, int limit = 10);
        Task<PipelineStatsDto> GetStatsAsync(int pipelineId);
        Task<AnalyticsSummaryDto> GetSummaryAsync(int totalPipelines);
        Task<IEnumerable<RunsByDayDto>> GetRunsByDayAsync(int days);

        /// <summary>
        /// Returns a dictionary keyed by pipeline ID containing the most recent
        /// PipelineRunLog for each pipeline. Used to show last-run
        /// status on the pipeline list page.
        /// </summary>
        Task<Dictionary<int, PipelineRunLog>> GetLatestRunPerPipelineAsync();
    }
}
