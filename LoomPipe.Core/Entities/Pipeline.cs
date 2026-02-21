using System;
using System.Collections.Generic;

namespace LoomPipe.Core.Entities
{
    /// <summary>
    /// Defines the ETL pipeline (sources, destinations, mappings, transformations).
    /// </summary>
    public class Pipeline
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public DateTime CreatedAt { get; set; }
        public DataSourceConfig Source { get; set; } = new DataSourceConfig();
        public DataSourceConfig Destination { get; set; } = new DataSourceConfig();
        public List<FieldMap> FieldMappings { get; set; } = new List<FieldMap>();
        public List<string> Transformations { get; set; } = new List<string>(); // Could be expressions, function names, etc.
        public Dictionary<string, object> Metadata { get; set; } = new Dictionary<string, object>(); // Additional pipeline metadata

        // Scheduling
        public bool ScheduleEnabled { get; set; }
        public string? CronExpression { get; set; }
        public DateTime? NextRunAt { get; set; }

        // Batch writing
        public int? BatchSize { get; set; }
        public int? BatchDelaySeconds { get; set; }
    }
}
