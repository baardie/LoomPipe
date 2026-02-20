using System;
using System.Collections.Generic;

namespace LoomPipe.Core.Exceptions
{
    /// <summary>
    /// Thrown when a pipeline fails at any stage of execution.
    /// The <see cref="Stage"/> property identifies which phase failed
    /// and <see cref="GetDetailedMessage"/> walks the full exception chain.
    /// </summary>
    public class PipelineExecutionException : Exception
    {
        /// <summary>The execution stage at which the failure occurred (e.g. "SourceRead", "Mapping", "DestinationWrite").</summary>
        public string? Stage { get; }

        public PipelineExecutionException(string message, Exception innerException, string? stage = null)
            : base(message, innerException)
        {
            Stage = stage;
        }

        /// <summary>
        /// Returns a flat, human-readable summary by walking the complete exception chain,
        /// joined with " → " so all context is visible without requiring a debugger.
        /// </summary>
        public string GetDetailedMessage()
        {
            var parts = new List<string>();
            Exception? current = this;
            while (current != null)
            {
                if (!string.IsNullOrWhiteSpace(current.Message))
                    parts.Add(current.Message);
                current = current.InnerException;
            }
            return string.Join(" → ", parts);
        }
    }
}
