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
            var logs = await _db.PipelineRunLogs
                .Where(r => r.PipelineId == pipelineId)
                .ToListAsync();

            var finished = logs
                .Where(r => r.FinishedAt.HasValue)
                .ToList();

            double? avgMs = finished.Count > 0
                ? finished.Average(r => (r.FinishedAt!.Value - r.StartedAt).TotalMilliseconds)
                : null;

            return new PipelineStatsDto
            {
                TotalRuns    = logs.Count,
                SuccessCount = logs.Count(r => r.Status == "Success"),
                FailCount    = logs.Count(r => r.Status == "Failed"),
                LastRunAt    = logs.Count > 0 ? logs.Max(r => r.StartedAt) : null,
                AvgDurationMs = avgMs,
            };
        }

        public async Task<AnalyticsSummaryDto> GetSummaryAsync(int totalPipelines)
        {
            var now = DateTime.UtcNow;
            var all = await _db.PipelineRunLogs.ToListAsync();
            int total    = all.Count;
            int success  = all.Count(r => r.Status == "Success");
            int last24h  = all.Count(r => r.StartedAt >= now.AddHours(-24));
            int last7d   = all.Count(r => r.StartedAt >= now.AddDays(-7));

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
    }
}
