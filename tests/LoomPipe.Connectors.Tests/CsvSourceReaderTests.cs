using Xunit;
using LoomPipe.Core.Entities;
using Microsoft.Extensions.Logging;
using Moq;
using System.IO;
using System.Threading.Tasks;
using System.Linq;
using System.Collections.Generic;

namespace LoomPipe.Connectors.Tests
{
    public class CsvSourceReaderTests
    {
        private readonly Mock<ILogger<CsvSourceReader>> _loggerMock;
        private readonly CsvSourceReader _reader;
        private readonly string _testCsvPath;

        public CsvSourceReaderTests()
        {
            _loggerMock = new Mock<ILogger<CsvSourceReader>>();
            _reader = new CsvSourceReader(_loggerMock.Object);
            _testCsvPath = Path.GetTempFileName();
            File.WriteAllText(_testCsvPath, "Header1,Header2\nValue1,Value2\nValue3,Value4");
        }

        [Fact]
        public async Task ReadAsync_ShouldReadAllLines()
        {
            // Arrange
            var config = new DataSourceConfig { ConnectionString = _testCsvPath };

            // Act
            var result = await _reader.ReadAsync(config);

            // Assert
            Assert.Equal(2, result.Count());
        }

        [Fact]
        public async Task DiscoverSchemaAsync_ShouldReturnHeaders()
        {
            // Arrange
            var config = new DataSourceConfig { ConnectionString = _testCsvPath };

            // Act
            var result = await _reader.DiscoverSchemaAsync(config);

            // Assert
            Assert.Equal(new[] { "Header1", "Header2" }, result);
        }

        [Fact]
        public async Task DryRunPreviewAsync_ShouldReturnSample()
        {
            // Arrange
            var config = new DataSourceConfig { ConnectionString = _testCsvPath };

            // Act
            var result = await _reader.DryRunPreviewAsync(config, 1);

            // Assert
            Assert.Single(result);
        }
    }
}
