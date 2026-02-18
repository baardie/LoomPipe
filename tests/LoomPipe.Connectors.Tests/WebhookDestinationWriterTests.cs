using Xunit;
using Moq;
using LoomPipe.Core.Entities;
using Microsoft.Extensions.Logging;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Dynamic;
using System.Net.Http;
using System.Net;
using Moq.Protected;
using System.Threading;
using System;

namespace LoomPipe.Connectors.Tests
{
    public class WebhookDestinationWriterTests
    {
        private readonly Mock<ILogger<WebhookDestinationWriter>> _loggerMock;
        private readonly Mock<HttpMessageHandler> _httpMessageHandlerMock;
        private readonly HttpClient _httpClient;
        private readonly WebhookDestinationWriter _writer;

        public WebhookDestinationWriterTests()
        {
            _loggerMock = new Mock<ILogger<WebhookDestinationWriter>>();
            _httpMessageHandlerMock = new Mock<HttpMessageHandler>();
            _httpClient = new HttpClient(_httpMessageHandlerMock.Object);
            _writer = new WebhookDestinationWriter(_httpClient, _loggerMock.Object);
        }

        [Fact]
        public async Task WriteAsync_ShouldPostDataToWebhook()
        {
            // Arrange
            var config = new DataSourceConfig { ConnectionString = "http://test.com/webhook" };
            dynamic item = new ExpandoObject();
            item.a = "test";
            var data = new List<object> { item };

            _httpMessageHandlerMock.Protected()
                .Setup<Task<HttpResponseMessage>>(
                    "SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>()
                )
                .ReturnsAsync(new HttpResponseMessage
                {
                    StatusCode = HttpStatusCode.OK,
                })
                .Verifiable();

            // Act
            await _writer.WriteAsync(config, data);

            // Assert
            _httpMessageHandlerMock.Protected().Verify(
                "SendAsync",
                Times.Once(),
                ItExpr.Is<HttpRequestMessage>(req =>
                    req.Method == HttpMethod.Post
                    && req.RequestUri == new Uri("http://test.com/webhook")
                ),
                ItExpr.IsAny<CancellationToken>()
            );
        }
    }
}
