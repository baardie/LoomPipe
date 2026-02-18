using LoomPipe.Core.Enums;
using System.Collections.Generic;

namespace LoomPipe.Core.Entities
{
    public class Connector
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public ConnectorType Type { get; set; }
        public string AssemblyQualifiedName { get; set; } = string.Empty;
        public ICollection<ConnectorParameter> Parameters { get; set; } = new List<ConnectorParameter>();
        public ConnectorStatus Status { get; set; }
    }
}
