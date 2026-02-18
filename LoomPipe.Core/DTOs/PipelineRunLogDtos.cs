using System;
using System.Collections.Generic;

namespace LoomPipe.Core.DTOs
{
    public class PipelineRunLogDto
    {
        public int Id { get; set; }
        public int PipelineId { get; set; }
        public DateTime StartedAt { get; set; }
        public DateTime? FinishedAt { get; set; }
        public long? DurationMs { get; set; }
        public string Status { get; set; } = string.Empty;
        public int RowsProcessed { get; set; }
        public string? ErrorMessage { get; set; }
        public string TriggeredBy { get; set; } = string.Empty;
    }

    public class PipelineStatsDto
    {
        public int TotalRuns { get; set; }
        public int SuccessCount { get; set; }
        public int FailCount { get; set; }
        public DateTime? LastRunAt { get; set; }
        public double? AvgDurationMs { get; set; }
    }

    public class AnalyticsSummaryDto
    {
        public int TotalPipelines { get; set; }
        public int TotalRuns { get; set; }
        public double SuccessRate { get; set; }
        public int RunsLast24h { get; set; }
        public int RunsLast7d { get; set; }
    }

    public class RunsByDayDto
    {
        public string Date { get; set; } = string.Empty; // yyyy-MM-dd
        public int RunCount { get; set; }
        public int SuccessCount { get; set; }
    }
}
