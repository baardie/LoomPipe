namespace LoomPipe.Core.DTOs
{
    // ── Request DTOs ──────────────────────────────────────────────────────────

    public class CreateConnectionProfileDto
    {
        public string Name { get; set; } = string.Empty;
        public string Provider { get; set; } = string.Empty;

        // Non-sensitive
        public string Host { get; set; } = string.Empty;
        public int? Port { get; set; }
        public string DatabaseName { get; set; } = string.Empty;
        public string Username { get; set; } = string.Empty;
        public string AdditionalConfig { get; set; } = "{}";

        // Sensitive — accepted in plaintext, encrypted before storage
        public string? Password { get; set; }
        public string? ApiKey { get; set; }
        public string? ServiceAccountJson { get; set; }
    }

    public class UpdateConnectionProfileDto
    {
        public string Name { get; set; } = string.Empty;
        public string Provider { get; set; } = string.Empty;

        // Non-sensitive
        public string Host { get; set; } = string.Empty;
        public int? Port { get; set; }
        public string DatabaseName { get; set; } = string.Empty;
        public string Username { get; set; } = string.Empty;
        public string AdditionalConfig { get; set; } = "{}";

        // Sensitive — null means "keep existing encrypted value"
        public string? Password { get; set; }
        public string? ApiKey { get; set; }
        public string? ServiceAccountJson { get; set; }
    }

    // ── Response DTOs (no secrets ever returned) ──────────────────────────────

    public class ConnectionProfileSummary
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public string Provider { get; set; } = string.Empty;
        public string Host { get; set; } = string.Empty;
        public int? Port { get; set; }
        public string DatabaseName { get; set; } = string.Empty;
        public string Username { get; set; } = string.Empty;
        public string AdditionalConfig { get; set; } = "{}";
        public DateTime CreatedAt { get; set; }
        public DateTime? LastTestedAt { get; set; }
        public bool LastTestSucceeded { get; set; }
    }

    public class ConnectionTestResult
    {
        public bool Success { get; set; }
        public string? ErrorMessage { get; set; }
        public long ElapsedMs { get; set; }
    }
}
