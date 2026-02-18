namespace LoomPipe.Core.Entities
{
    public class ConnectorParameter
    {
        public int Id { get; set; }
        public string Key { get; set; } = string.Empty;
        public string Value { get; set; } = string.Empty;
        public int ConnectorId { get; set; }
        public Connector Connector { get; set; } = null!;
    }
}
