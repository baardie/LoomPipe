using System;
using System.IO;
using System.Net;
using System.Net.Mail;
using System.Text.Json;
using System.Threading.Tasks;
using LoomPipe.Core.Interfaces;
using LoomPipe.Core.Settings;
using Microsoft.Extensions.Logging;

namespace LoomPipe.Services
{
    /// <summary>
    /// SMTP-based implementation of <see cref="IEmailNotificationService"/>.
    /// Settings are persisted to <c>email-settings.json</c> in the application content root
    /// so that an admin can update them at runtime without restarting the server.
    /// </summary>
    public class EmailNotificationService : IEmailNotificationService
    {
        private readonly ILogger<EmailNotificationService> _logger;
        private readonly string _settingsPath;

        private static readonly JsonSerializerOptions _jsonOpts = new()
        {
            WriteIndented            = true,
            PropertyNameCaseInsensitive = true,
        };

        public EmailNotificationService(ILogger<EmailNotificationService> logger, string settingsPath)
        {
            _logger      = logger;
            _settingsPath = settingsPath;
        }

        // ── Settings persistence ──────────────────────────────────────────────

        public EmailSettings GetSettings()
        {
            try
            {
                if (File.Exists(_settingsPath))
                {
                    var json = File.ReadAllText(_settingsPath);
                    return JsonSerializer.Deserialize<EmailSettings>(json, _jsonOpts) ?? new EmailSettings();
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Could not load email settings from '{Path}'. Using defaults.", _settingsPath);
            }
            return new EmailSettings();
        }

        public void SaveSettings(EmailSettings settings)
        {
            var json = JsonSerializer.Serialize(settings, _jsonOpts);
            File.WriteAllText(_settingsPath, json);
            _logger.LogInformation("Email settings saved to '{Path}'.", _settingsPath);
        }

        // ── Notifications ─────────────────────────────────────────────────────

        public async Task SendPipelineFailureAsync(
            string pipelineName, int pipelineId,
            string errorMessage, string stage,
            string triggeredBy, DateTime failedAt)
        {
            var cfg = GetSettings();
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
            var cfg = GetSettings();
            if (!cfg.Enabled || !cfg.NotifyOnSuccess || string.IsNullOrWhiteSpace(cfg.AdminEmail))
                return;

            var subject = $"[LoomPipe] Pipeline Succeeded: {pipelineName}";
            var body    = BuildSuccessBody(pipelineName, pipelineId, rowsProcessed, triggeredBy, completedAt);

            await SendAsync(cfg, subject, body);
        }

        public async Task<bool> TestConnectionAsync()
        {
            var cfg = GetSettings();
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
