using System.Threading.Tasks;
using LoomPipe.Core.Interfaces;
using LoomPipe.Core.Settings;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;

namespace LoomPipe.Server.Controllers
{
    /// <summary>
    /// Admin-only API for reading and writing runtime settings (email notifications, etc.).
    /// Settings are persisted to <c>email-settings.json</c> in the server content root.
    /// </summary>
    [ApiController]
    [Route("api/admin/settings")]
    [Authorize(Roles = "Admin")]
    public class AdminSettingsController : ControllerBase
    {
        private readonly IEmailNotificationService _emailService;
        private readonly ILogger<AdminSettingsController> _logger;

        public AdminSettingsController(
            IEmailNotificationService emailService,
            ILogger<AdminSettingsController> logger)
        {
            _emailService = emailService;
            _logger       = logger;
        }

        // ── Email settings ────────────────────────────────────────────────────

        /// <summary>Returns the current email notification settings. The SMTP password is masked in the response.</summary>
        [HttpGet("email")]
        public IActionResult GetEmailSettings()
        {
            var settings = _emailService.GetSettings();

            // Return a safe view — mask the password so it is not leaked to the browser
            return Ok(new
            {
                settings.Enabled,
                settings.SmtpHost,
                settings.SmtpPort,
                settings.EnableSsl,
                settings.Username,
                PasswordSet      = !string.IsNullOrWhiteSpace(settings.Password),
                settings.FromAddress,
                settings.FromName,
                settings.AdminEmail,
                settings.NotifyOnFailure,
                settings.NotifyOnSuccess,
            });
        }

        /// <summary>
        /// Persists updated email notification settings.
        /// Send an empty string for <c>Password</c> to keep the existing saved password.
        /// </summary>
        [HttpPut("email")]
        public IActionResult SaveEmailSettings([FromBody] EmailSettings incoming)
        {
            // If the client sends a blank password, keep the previously stored one
            if (string.IsNullOrWhiteSpace(incoming.Password))
            {
                var existing = _emailService.GetSettings();
                incoming.Password = existing.Password;
            }

            _emailService.SaveSettings(incoming);
            _logger.LogInformation("Email notification settings updated by {User}.",
                User.Identity?.Name ?? "unknown");

            return NoContent();
        }

        /// <summary>
        /// Sends a test email using the current settings.
        /// Returns 200 with <c>{ "success": true }</c> on success, or 400 on SMTP error.
        /// </summary>
        [HttpPost("email/test")]
        public async Task<IActionResult> TestEmailSettings()
        {
            var success = await _emailService.TestConnectionAsync();
            if (success)
                return Ok(new { success = true, message = "Test email sent successfully." });

            return BadRequest(new { success = false, message = "Failed to send test email. Check the SMTP settings and server logs for details." });
        }
    }
}
