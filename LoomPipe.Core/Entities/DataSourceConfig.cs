using System.Collections.Generic;

namespace LoomPipe.Core.Entities
{
    /// <summary>
    /// Represents configuration for a data source (type, connection details, schema info).
    /// </summary>
    public class DataSourceConfig
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public string Type { get; set; } = string.Empty; // e.g., REST, SQL, CSV, etc.
        public string ConnectionString { get; set; } = string.Empty;
        public Dictionary<string, object> Parameters { get; set; } = new Dictionary<string, object>(); // Additional connection details
        public string Schema { get; set; } = string.Empty; // Optional: JSON or other schema representation
    }
}
