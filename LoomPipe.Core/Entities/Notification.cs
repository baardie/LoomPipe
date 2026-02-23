using System;

namespace LoomPipe.Core.Entities
{
    public class Notification
    {
        public int Id { get; set; }

        /// <summary>
        /// Dot-namespaced type token, e.g. "pipeline.success", "pipeline.failed".
        /// New categories can be added by convention without schema changes.
        /// </summary>
        public string Type { get; set; } = string.Empty;

        public string Title { get; set; } = string.Empty;
        public string Message { get; set; } = string.Empty;

        /// <summary>Optional FK to the pipeline that produced this notification.</summary>
        public int? PipelineId { get; set; }
        public Pipeline? Pipeline { get; set; }

        public DateTime CreatedAt { get; set; }
        public bool IsRead { get; set; }
    }
}
