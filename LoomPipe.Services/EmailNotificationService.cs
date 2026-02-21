using System;
using System.Net;
using System.Net.Mail;
using System.Threading.Tasks;
using LoomPipe.Core.Entities;
using LoomPipe.Core.Interfaces;
using LoomPipe.Core.Settings;
using LoomPipe.Storage.Interfaces;
using Microsoft.AspNetCore.DataProtection;
using Microsoft.Extensions.Logging;

namespace LoomPipe.Services
{
    /// <summary>
    /// SMTP-based implementation of <see cref="IEmailNotificationService"/>.
    /// Settings are stored in the database; the SMTP password is encrypted at rest
    /// using ASP.NET Core Data Protection.
    /// </summary>
    public class EmailNotificationService : IEmailNotificationService
    {
        private const string Purpose = "LoomPipe.SmtpSettings.v1";

        private readonly ISmtpSettingsRepository _repo;
        private readonly IDataProtector _protector;
        private readonly ILogger<EmailNotificationService> _logger;

        public EmailNotificationService(
            ISmtpSettingsRepository repo,
            IDataProtectionProvider dpProvider,
            ILogger<EmailNotificationService> logger)
        {
            _repo      = repo;
            _protector = dpProvider.CreateProtector(Purpose);
            _logger    = logger;
        }

        // ── Settings persistence ──────────────────────────────────────────────

        public async Task<EmailSettings> GetSettingsAsync()
        {
            var row = await _repo.GetAsync();
            if (row == null) return new EmailSettings();

            return new EmailSettings
            {
                Enabled         = row.Enabled,
                SmtpHost        = row.SmtpHost,
                SmtpPort        = row.SmtpPort,
                EnableSsl       = row.EnableSsl,
                Username        = row.Username,
                Password        = DecryptPassword(row.EncryptedPassword),
                FromAddress     = row.FromAddress,
                FromName        = row.FromName,
                AdminEmail      = row.AdminEmail,
                NotifyOnFailure = row.NotifyOnFailure,
                NotifyOnSuccess = row.NotifyOnSuccess,
            };
        }

        public async Task SaveSettingsAsync(EmailSettings settings)
        {
            var row = new SmtpSettings
            {
                Enabled           = settings.Enabled,
                SmtpHost          = settings.SmtpHost,
                SmtpPort          = settings.SmtpPort,
                EnableSsl         = settings.EnableSsl,
                Username          = settings.Username,
                EncryptedPassword = EncryptPassword(settings.Password),
                FromAddress       = settings.FromAddress,
                FromName          = settings.FromName,
                AdminEmail        = settings.AdminEmail,
                NotifyOnFailure   = settings.NotifyOnFailure,
                NotifyOnSuccess   = settings.NotifyOnSuccess,
            };

            await _repo.SaveAsync(row);
            _logger.LogInformation("Email settings saved to database.");
        }

        // ── Notifications ─────────────────────────────────────────────────────

        public async Task SendPipelineFailureAsync(
            string pipelineName, int pipelineId,
            string errorMessage, string stage,
            string triggeredBy, DateTime failedAt)
        {
            var cfg = await GetSettingsAsync();
            if (!cfg.Enabled || !cfg.NotifyOnFailure || string.IsNullOrWhiteSpace(cfg.AdminEmail))
                return;

            var subject = $"[LoomPipe] Pipeline Failed: {pipelineName}";
            var body    = BuildFailureBody(pipelineName, pipelineId, errorMessage, stage, triggeredBy, failedAt);
            await SendAsync(cfg, subject, body);
        }

        public async Task SendPipelineSuccessAsync(
            string pipelineName, int pipelineId,
            int rowsProcessed, string triggeredBy, DateTime completedAt)
        {
            var cfg = await GetSettingsAsync();
            if (!cfg.Enabled || !cfg.NotifyOnSuccess || string.IsNullOrWhiteSpace(cfg.AdminEmail))
                return;

            var subject = $"[LoomPipe] Pipeline Succeeded: {pipelineName}";
            var body    = BuildSuccessBody(pipelineName, pipelineId, rowsProcessed, triggeredBy, completedAt);
            await SendAsync(cfg, subject, body);
        }

        public async Task<bool> TestConnectionAsync()
        {
            var cfg = await GetSettingsAsync();
            try
            {
                var subject = "[LoomPipe] Email Configuration Test";
                var body    = $@"<p style=""font-family:sans-serif"">
                    This is a test email from <strong>LoomPipe</strong> confirming that your SMTP settings are working correctly.
                    </p><p style=""font-family:sans-serif;color:#6b7280"">Sent at: {DateTime.UtcNow:yyyy-MM-dd HH:mm:ss} UTC</p>";

                await SendAsync(cfg, subject, body);
                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Email test failed.");
                return false;
            }
        }

        // ── SMTP dispatch ─────────────────────────────────────────────────────

        private async Task SendAsync(EmailSettings cfg, string subject, string htmlBody)
        {
#pragma warning disable SYSLIB0006
            using var client = new SmtpClient(cfg.SmtpHost, cfg.SmtpPort)
            {
                EnableSsl      = cfg.EnableSsl,
                DeliveryMethod = SmtpDeliveryMethod.Network,
                Timeout        = 15_000,
                Credentials    = string.IsNullOrWhiteSpace(cfg.Username)
                    ? null
                    : new NetworkCredential(cfg.Username, cfg.Password),
            };
#pragma warning restore SYSLIB0006

            var from = new MailAddress(cfg.FromAddress, cfg.FromName);
            var to   = new MailAddress(cfg.AdminEmail);

            using var message = new MailMessage(from, to)
            {
                Subject    = subject,
                Body       = htmlBody,
                IsBodyHtml = true,
            };

            try
            {
                await client.SendMailAsync(message);
                _logger.LogInformation("Email '{Subject}' delivered to {AdminEmail}.", subject, cfg.AdminEmail);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "SMTP delivery failed for '{Subject}' → {AdminEmail}.", subject, cfg.AdminEmail);
                throw;
            }
        }

        // ── Encryption helpers ────────────────────────────────────────────────

        private string EncryptPassword(string? password)
        {
            if (string.IsNullOrEmpty(password)) return string.Empty;
            return _protector.Protect(password);
        }

        private string DecryptPassword(string encrypted)
        {
            if (string.IsNullOrEmpty(encrypted)) return string.Empty;
            try
            {
                return _protector.Unprotect(encrypted);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to decrypt SMTP password.");
                return string.Empty;
            }
        }

        // ── Email bodies ──────────────────────────────────────────────────────

        private static string Encode(string? s) => WebUtility.HtmlEncode(s ?? string.Empty);

        private static string BuildFailureBody(
            string pipelineName, int pipelineId,
            string errorMessage, string stage,
            string triggeredBy, DateTime failedAt)
        {
            var stageLabel = stage switch
            {
                "SourceRead"       => "Source Read",
                "Mapping"          => "Field Mapping / Transformation",
                "DestinationWrite" => "Destination Write",
                _                  => stage,
            };

            return $@"<!DOCTYPE html>
<html>
<head><meta charset=""utf-8"">
<style>
  body  {{ font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#f1f5f9;margin:0;padding:20px }}
  .card {{ background:#fff;border-radius:8px;padding:28px;max-width:580px;margin:0 auto;border-top:4px solid #ef4444 }}
  h2   {{ color:#ef4444;margin:0 0 20px;font-size:17px;letter-spacing:-.3px }}
  .row {{ margin-bottom:14px }}
  .lbl {{ font-size:10px;text-transform:uppercase;letter-spacing:.06em;color:#94a3b8;margin-bottom:3px }}
  .val {{ font-size:14px;color:#1e293b }}
  .err {{ background:#fef2f2;border:1px solid #fecaca;border-radius:6px;padding:12px 14px;
          font-family:monospace;font-size:12px;color:#b91c1c;word-break:break-word;line-height:1.6 }}
  .ft  {{ margin-top:22px;font-size:11px;color:#94a3b8 }}
</style>
</head>
<body>
<div class=""card"">
  <h2>&#x26A0;&#xFE0F; Pipeline Execution Failed</h2>
  <div class=""row""><div class=""lbl"">Pipeline</div>
    <div class=""val"">{Encode(pipelineName)} <span style=""color:#94a3b8"">(ID&nbsp;{pipelineId})</span></div></div>
  <div class=""row""><div class=""lbl"">Failed Stage</div><div class=""val"">{Encode(stageLabel)}</div></div>
  <div class=""row""><div class=""lbl"">Failed At</div><div class=""val"">{failedAt:yyyy-MM-dd HH:mm:ss} UTC</div></div>
  <div class=""row""><div class=""lbl"">Triggered By</div><div class=""val"">{Encode(triggeredBy)}</div></div>
  <div class=""row""><div class=""lbl"">Error Detail</div>
    <div class=""err"">{Encode(errorMessage)}</div></div>
  <div class=""ft"">This notification was sent by LoomPipe. Log in to view the full run history and diagnose the issue.</div>
</div>
</body>
</html>";
        }

        private static string BuildSuccessBody(
            string pipelineName, int pipelineId,
            int rowsProcessed, string triggeredBy, DateTime completedAt)
        {
            return $@"<!DOCTYPE html>
<html>
<head><meta charset=""utf-8"">
<style>
  body  {{ font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#f1f5f9;margin:0;padding:20px }}
  .card {{ background:#fff;border-radius:8px;padding:28px;max-width:580px;margin:0 auto;border-top:4px solid #22c55e }}
  h2   {{ color:#16a34a;margin:0 0 20px;font-size:17px;letter-spacing:-.3px }}
  .row {{ margin-bottom:14px }}
  .lbl {{ font-size:10px;text-transform:uppercase;letter-spacing:.06em;color:#94a3b8;margin-bottom:3px }}
  .val {{ font-size:14px;color:#1e293b }}
  .ft  {{ margin-top:22px;font-size:11px;color:#94a3b8 }}
</style>
</head>
<body>
<div class=""card"">
  <h2>&#x2705; Pipeline Completed Successfully</h2>
  <div class=""row""><div class=""lbl"">Pipeline</div>
    <div class=""val"">{Encode(pipelineName)} <span style=""color:#94a3b8"">(ID&nbsp;{pipelineId})</span></div></div>
  <div class=""row""><div class=""lbl"">Completed At</div><div class=""val"">{completedAt:yyyy-MM-dd HH:mm:ss} UTC</div></div>
  <div class=""row""><div class=""lbl"">Rows Processed</div><div class=""val"">{rowsProcessed:N0}</div></div>
  <div class=""row""><div class=""lbl"">Triggered By</div><div class=""val"">{Encode(triggeredBy)}</div></div>
  <div class=""ft"">This notification was sent by LoomPipe.</div>
</div>
</body>
</html>";
        }
    }
}
