using System;

namespace LoomPipe.Core.Entities
{
    public class PipelineRunLog
    {
        public int Id { get; set; }
        public int PipelineId { get; set; }
        public Pipeline Pipeline { get; set; } = null!;
        public DateTime StartedAt { get; set; }
        public DateTime? FinishedAt { get; set; }
        public string Status { get; set; } = "Running"; // Running | Success | Failed
        public int RowsProcessed { get; set; }
        public string? ErrorMessage { get; set; }
        public string TriggeredBy { get; set; } = string.Empty;

        /// <summary>
        /// JSON snapshot of the pipeline configuration captured at the moment this run
        /// was triggered. Used by the retry feature to re-execute with identical settings
        /// even if the pipeline was edited afterward. Null once the retention window expires.
        /// </summary>
        public string? ConfigSnapshot { get; set; }

        /// <summary>UTC timestamp after which ConfigSnapshot is considered expired and will be cleared.</summary>
        public DateTime? SnapshotExpiresAt { get; set; }

        /// <summary>If this run is a retry, the Id of the original failed run.</summary>
        public int? RetryOfRunId { get; set; }
    }
}
