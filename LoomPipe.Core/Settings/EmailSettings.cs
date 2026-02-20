namespace LoomPipe.Core.Settings
{
    /// <summary>
    /// SMTP / notification configuration for pipeline event emails.
    /// Persisted to <c>email-settings.json</c> in the content root and editable
    /// at runtime via the admin settings API.
    /// </summary>
    public class EmailSettings
    {
        /// <summary>Master switch — no emails are sent when false.</summary>
        public bool Enabled { get; set; } = false;

        // ── SMTP connection ─────────────────────────────────────────────────
        public string SmtpHost { get; set; } = string.Empty;
        public int    SmtpPort { get; set; } = 587;
        public bool   EnableSsl { get; set; } = true;
        public string Username { get; set; } = string.Empty;
        /// <summary>SMTP password. Stored in plain text in email-settings.json — keep the file out of source control.</summary>
        public string Password { get; set; } = string.Empty;

        // ── Sender ─────────────────────────────────────────────────────────
        public string FromAddress { get; set; } = string.Empty;
        public string FromName    { get; set; } = "LoomPipe";

        // ── Recipient ──────────────────────────────────────────────────────
        /// <summary>Admin email address that receives pipeline notifications.</summary>
        public string AdminEmail { get; set; } = string.Empty;

        // ── Event triggers ─────────────────────────────────────────────────
        public bool NotifyOnFailure { get; set; } = true;
        public bool NotifyOnSuccess { get; set; } = false;
    }
}
