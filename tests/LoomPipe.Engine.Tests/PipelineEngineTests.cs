using Xunit;
using Moq;
using LoomPipe.Core.Interfaces;
using LoomPipe.Core.Entities;
using Microsoft.Extensions.Logging;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;

namespace LoomPipe.Engine.Tests
{
    public class PipelineEngineTests
    {
        private readonly Mock<ISourceReader> _sourceReaderMock;
        private readonly Mock<IDestinationWriter> _destinationWriterMock;
        private readonly Mock<ILogger<PipelineEngine>> _loggerMock;
        private readonly PipelineEngine _engine;

        public PipelineEngineTests()
        {
            _sourceReaderMock = new Mock<ISourceReader>();
            _destinationWriterMock = new Mock<IDestinationWriter>();
            _loggerMock = new Mock<ILogger<PipelineEngine>>();
            _engine = new PipelineEngine(_sourceReaderMock.Object, _destinationWriterMock.Object, _loggerMock.Object);
        }

        [Fact]
        public async Task RunPipelineAsync_ShouldCallReadWrite()
        {
            // Arrange
            var pipeline = new Pipeline();
            var sourceData = new List<object> { new ExpandoObject() };
            _sourceReaderMock.Setup(r => r.ReadAsync(It.IsAny<DataSourceConfig>())).ReturnsAsync(sourceData);

            // Act
            await _engine.RunPipelineAsync(pipeline);

            // Assert
            _sourceReaderMock.Verify(r => r.ReadAsync(pipeline.Source), Times.Once);
            _destinationWriterMock.Verify(w => w.WriteAsync(pipeline.Destination, It.IsAny<IEnumerable<object>>()), Times.Once);
        }

        [Fact]
        public async Task RunPipelineAsync_ShouldApplyMappingsAndTransformations()
        {
            // Arrange
            var pipeline = new Pipeline
            {
                FieldMappings = new List<FieldMap>
                {
                    new FieldMap { SourceField = "a", DestinationField = "b" }
                },
                Transformations = new List<string>
                {
                    "b = b + '_transformed'"
                }
            };
            dynamic sourceItem = new ExpandoObject();
            sourceItem.a = "test";
            var sourceData = new List<object> { sourceItem };
            _sourceReaderMock.Setup(r => r.ReadAsync(It.IsAny<DataSourceConfig>())).ReturnsAsync(sourceData);

            IEnumerable<object> writtenData = null;
            _destinationWriterMock.Setup(w => w.WriteAsync(It.IsAny<DataSourceConfig>(), It.IsAny<IEnumerable<object>>()))
                .Callback<DataSourceConfig, IEnumerable<object>>((config, data) => writtenData = data)
                .Returns(Task.CompletedTask);

            // Act
            await _engine.RunPipelineAsync(pipeline);

            // Assert
            Assert.NotNull(writtenData);
            Assert.Single(writtenData);
            dynamic writtenItem = writtenData.First();
            Assert.True(((IDictionary<string, object>)writtenItem).ContainsKey("b"));
            Assert.Equal("test_transformed", writtenItem.b);
        }

        [Fact]
        public async Task DryRunAsync_ShouldReturnTransformedData()
        {
            // Arrange
            var pipeline = new Pipeline
            {
                FieldMappings = new List<FieldMap>
                {
                    new FieldMap { SourceField = "a", DestinationField = "b" }
                }
            };
            dynamic sourceItem = new ExpandoObject();
            sourceItem.a = "test";
            var sourceData = new List<object> { sourceItem };
            _sourceReaderMock.Setup(r => r.DryRunPreviewAsync(It.IsAny<DataSourceConfig>(), It.IsAny<int>())).ReturnsAsync(sourceData);

            // Act
            var result = await _engine.DryRunAsync(pipeline);

            // Assert
            _sourceReaderMock.Verify(r => r.DryRunPreviewAsync(pipeline.Source, It.IsAny<int>()), Times.Once);
            Assert.NotNull(result);
            Assert.Single(result.SourcePreview);
            Assert.Single(result.MappedPreview);
            Assert.Single(result.TransformedPreview);

            dynamic mappedItem = result.MappedPreview.First();
            Assert.True(((IDictionary<string, object>)mappedItem).ContainsKey("b"));
            Assert.Equal("test", mappedItem.b);
        }
    }
}
