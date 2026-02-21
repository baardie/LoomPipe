namespace LoomPipe.Core.Entities
{
    /// <summary>
    /// Singleton settings row (Id always = 1) that stores SMTP configuration.
    /// The SMTP password is stored encrypted via ASP.NET Core Data Protection —
    /// it is never persisted in plain text.
    /// </summary>
    public class SmtpSettings
    {
        public int Id { get; set; } = 1;

        /// <summary>Master switch — no emails are sent when false.</summary>
        public bool Enabled { get; set; } = false;

        // ── SMTP connection ─────────────────────────────────────────────────
        public string SmtpHost { get; set; } = string.Empty;
        public int    SmtpPort { get; set; } = 587;
        public bool   EnableSsl { get; set; } = true;
        public string Username { get; set; } = string.Empty;

        /// <summary>SMTP password encrypted with ASP.NET Core Data Protection.</summary>
        public string EncryptedPassword { get; set; } = string.Empty;

        // ── Sender ─────────────────────────────────────────────────────────
        public string FromAddress { get; set; } = string.Empty;
        public string FromName    { get; set; } = "LoomPipe";

        // ── Recipient ──────────────────────────────────────────────────────
        public string AdminEmail { get; set; } = string.Empty;

        // ── Event triggers ─────────────────────────────────────────────────
        public bool NotifyOnFailure { get; set; } = true;
        public bool NotifyOnSuccess { get; set; } = false;
    }
}
