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
    }
}
