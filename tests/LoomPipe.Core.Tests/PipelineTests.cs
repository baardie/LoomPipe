using Xunit;
using LoomPipe.Core.Entities;

namespace LoomPipe.Core.Tests
{
    public class PipelineTests
    {
        [Fact]
        public void Pipeline_CanBeCreated()
        {
            // Arrange
            var pipeline = new Pipeline
            {
                Name = "Test Pipeline"
            };

            // Assert
            Assert.Equal("Test Pipeline", pipeline.Name);
        }
    }
}
