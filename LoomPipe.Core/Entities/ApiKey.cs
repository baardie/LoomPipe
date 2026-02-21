using System;

namespace LoomPipe.Core.Entities
{
    /// <summary>
    /// Represents a hashed API key that can be used in place of a JWT token
    /// by sending the raw key value in the  X-Api-Key  request header.
    /// The raw key is shown to the user exactly once on creation and is never
    /// stored — only its SHA-256 hex digest is persisted.
    /// </summary>
    public class ApiKey
    {
        public int      Id          { get; set; }
        public int      AppUserId   { get; set; }
        public string   Name        { get; set; } = string.Empty;  // friendly label
        public string   KeyHash     { get; set; } = string.Empty;  // SHA-256 hex (lowercase)
        public bool     IsActive    { get; set; } = true;
        public DateTime CreatedAt   { get; set; }
        public DateTime? LastUsedAt { get; set; }
        public DateTime? ExpiresAt  { get; set; }

        // Navigation
        public AppUser? AppUser { get; set; }
    }
}
