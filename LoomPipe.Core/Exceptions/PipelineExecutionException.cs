using System;

namespace LoomPipe.Core.Exceptions
{
    public class PipelineExecutionException : Exception
    {
        public PipelineExecutionException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }
}
