namespace LoomPipe.Core.Entities
{
    /// <summary>
    /// Stores connection metadata for a database or data service.
    /// Sensitive credentials are encrypted in EncryptedSecrets — never stored in plaintext.
    /// </summary>
    public class ConnectionProfile
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;

        /// <summary>
        /// Provider key: sqlserver | postgresql | mysql | oracle | mongodb | neo4j | snowflake | bigquery | pinecone | milvus
        /// </summary>
        public string Provider { get; set; } = string.Empty;

        // ── Non-sensitive display fields ──────────────────────────────────────
        public string Host { get; set; } = string.Empty;
        public int? Port { get; set; }
        public string DatabaseName { get; set; } = string.Empty;
        public string Username { get; set; } = string.Empty;

        /// <summary>
        /// Non-sensitive extra config as JSON (e.g. warehouse, schema, project ID, index name, collection).
        /// </summary>
        public string AdditionalConfig { get; set; } = "{}";

        // ── Sensitive — AES-256-CBC via ASP.NET Core Data Protection ──────────
        /// <summary>
        /// Encrypted JSON containing: password, apiKey, serviceAccountJson (whichever apply).
        /// Never returned from the API.
        /// </summary>
        public string EncryptedSecrets { get; set; } = string.Empty;

        // ── Audit ──────────────────────────────────────────────────────────────
        public DateTime CreatedAt { get; set; }
        public DateTime? LastTestedAt { get; set; }
        public bool LastTestSucceeded { get; set; }
    }
}
