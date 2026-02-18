using System.Collections.Generic;

namespace LoomPipe.Core.Entities
{
    /// <summary>
    /// Maps source fields to destination fields, supports automap metadata.
    /// </summary>
    public class FieldMap
    {
        public int Id { get; set; }
        public string SourceField { get; set; } = string.Empty;
        public string DestinationField { get; set; } = string.Empty;
        public double? AutomapScore { get; set; } // For automap metadata (e.g., Levenshtein score)
        public bool IsAutomapped { get; set; }
        public Dictionary<string, object> Metadata { get; set; } = new Dictionary<string, object>(); // Additional mapping metadata
    }
}
