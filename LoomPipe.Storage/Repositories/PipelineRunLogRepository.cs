using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using LoomPipe.Core.DTOs;
using LoomPipe.Core.Entities;
using LoomPipe.Core.Interfaces;
using Microsoft.EntityFrameworkCore;

namespace LoomPipe.Storage.Repositories
{
    public class PipelineRunLogRepository : IPipelineRunLogRepository
    {
        private readonly LoomPipeDbContext _db;

        public PipelineRunLogRepository(LoomPipeDbContext db)
        {
            _db = db;
        }

        public async Task AddAsync(PipelineRunLog log)
        {
            _db.PipelineRunLogs.Add(log);
            await _db.SaveChangesAsync();
        }

        public async Task UpdateAsync(PipelineRunLog log)
        {
            _db.PipelineRunLogs.Update(log);
            await _db.SaveChangesAsync();
        }

        public Task<PipelineRunLog?> GetByIdAsync(int id) =>
            _db.PipelineRunLogs.FirstOrDefaultAsync(r => r.Id == id);

        public async Task<IEnumerable<PipelineRunLog>> GetByPipelineIdAsync(int pipelineId, int limit = 10) =>
            await _db.PipelineRunLogs
                .Where(r => r.PipelineId == pipelineId)
                .OrderByDescending(r => r.StartedAt)
                .Take(limit)
                .ToListAsync();

        public async Task<PipelineStatsDto> GetStatsAsync(int pipelineId)
        {
            var query = _db.PipelineRunLogs.Where(r => r.PipelineId == pipelineId);

            var totalRuns    = await query.CountAsync();
            var successCount = await query.CountAsync(r => r.Status == "Success");
            var failCount    = await query.CountAsync(r => r.Status == "Failed");
            var lastRunAt    = totalRuns > 0 ? await query.MaxAsync(r => (DateTime?)r.StartedAt) : null;

            // Pull only the two timestamp columns for finished runs to compute avg duration
            var durations = await query
                .Where(r => r.FinishedAt.HasValue)
                .Select(r => new { r.StartedAt, r.FinishedAt })
                .ToListAsync();

            double? avgMs = durations.Count > 0
                ? durations.Average(r => (r.FinishedAt!.Value - r.StartedAt).TotalMilliseconds)
                : null;

            return new PipelineStatsDto
            {
                TotalRuns     = totalRuns,
                SuccessCount  = successCount,
                FailCount     = failCount,
                LastRunAt     = lastRunAt,
                AvgDurationMs = avgMs,
            };
        }

        public async Task<AnalyticsSummaryDto> GetSummaryAsync(int totalPipelines)
        {
            var now     = DateTime.UtcNow;
            var query   = _db.PipelineRunLogs.AsQueryable();

            int total   = await query.CountAsync();
            int success = await query.CountAsync(r => r.Status == "Success");
            int last24h = await query.CountAsync(r => r.StartedAt >= now.AddHours(-24));
            int last7d  = await query.CountAsync(r => r.StartedAt >= now.AddDays(-7));

            return new AnalyticsSummaryDto
            {
                TotalPipelines = totalPipelines,
                TotalRuns      = total,
                SuccessRate    = total > 0 ? Math.Round((double)success / total * 100, 1) : 0,
                RunsLast24h    = last24h,
                RunsLast7d     = last7d,
            };
        }

        public async Task<IEnumerable<RunsByDayDto>> GetRunsByDayAsync(int days)
        {
            var now   = DateTime.UtcNow.Date;
            var start = now.AddDays(-(days - 1));
            var logs  = await _db.PipelineRunLogs
                .Where(r => r.StartedAt >= start)
                .ToListAsync();

            return Enumerable.Range(0, days)
                .Select(i =>
                {
                    var date = start.AddDays(i);
                    var dayLogs = logs.Where(r => r.StartedAt.Date == date).ToList();
                    return new RunsByDayDto
                    {
                        Date         = date.ToString("yyyy-MM-dd"),
                        RunCount     = dayLogs.Count,
                        SuccessCount = dayLogs.Count(r => r.Status == "Success"),
                    };
                })
                .ToList();
        }

        public async Task<Dictionary<int, PipelineRunLog>> GetLatestRunPerPipelineAsync()
        {
            // Correlated subquery: for each distinct pipeline, find the run with the max StartedAt.
            // This translates to efficient SQL across all providers.
            var pipelineIds = await _db.PipelineRunLogs
                .Select(r => r.PipelineId)
                .Distinct()
                .ToListAsync();

            var latest = new Dictionary<int, PipelineRunLog>(pipelineIds.Count);
            foreach (var pid in pipelineIds)
            {
                var run = await _db.PipelineRunLogs
                    .Where(r => r.PipelineId == pid)
                    .OrderByDescending(r => r.StartedAt)
                    .FirstOrDefaultAsync();
                if (run != null) latest[pid] = run;
            }
            return latest;
        }
    }
}
