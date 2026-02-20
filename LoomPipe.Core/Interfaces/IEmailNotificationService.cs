using System;
using System.Threading.Tasks;
using LoomPipe.Core.Settings;

namespace LoomPipe.Core.Interfaces
{
    /// <summary>
    /// Sends pipeline event notifications via SMTP and manages the persisted email configuration.
    /// </summary>
    public interface IEmailNotificationService
    {
        /// <summary>Returns the current email settings (loaded from disk).</summary>
        EmailSettings GetSettings();

        /// <summary>Persists updated email settings to disk.</summary>
        void SaveSettings(EmailSettings settings);

        /// <summary>Sends a failure notification to the configured admin email if enabled.</summary>
        Task SendPipelineFailureAsync(string pipelineName, int pipelineId, string errorMessage, string stage, string triggeredBy, DateTime failedAt);

        /// <summary>Sends a success notification to the configured admin email if enabled.</summary>
        Task SendPipelineSuccessAsync(string pipelineName, int pipelineId, int rowsProcessed, string triggeredBy, DateTime completedAt);

        /// <summary>
        /// Sends a test email using the current settings.
        /// Returns <c>true</c> if delivery succeeded, <c>false</c> on SMTP error.
        /// </summary>
        Task<bool> TestConnectionAsync();
    }
}
