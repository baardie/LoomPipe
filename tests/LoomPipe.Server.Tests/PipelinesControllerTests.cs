using LoomPipe.Core.Entities;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Net.Http.Json;
using System.Threading.Tasks;
using Xunit;

namespace LoomPipe.Server.Tests
{
    public class PipelinesControllerTests : IClassFixture<CustomWebApplicationFactory>
    {
        private readonly HttpClient _client;

        public PipelinesControllerTests(CustomWebApplicationFactory factory)
        {
            _client = factory.CreateClient();
        }

        [Fact]
        public async Task GetPipelines_ReturnsOkWithEmptyList()
        {
            var response = await _client.GetAsync("/api/pipelines");
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);

            var pipelines = await response.Content.ReadFromJsonAsync<List<Pipeline>>();
            Assert.NotNull(pipelines);
            Assert.Empty(pipelines);
        }

        [Fact]
        public async Task PostPipeline_ReturnsCreated()
        {
            var pipeline = new Pipeline
            {
                Name = "Test Pipeline",
                Source = new DataSourceConfig { Name = "src", Type = "csv", ConnectionString = "file.csv" },
                Destination = new DataSourceConfig { Name = "dest", Type = "webhook", ConnectionString = "http://example.com/hook" }
            };

            var response = await _client.PostAsJsonAsync("/api/pipelines", pipeline);
            Assert.Equal(HttpStatusCode.Created, response.StatusCode);

            var created = await response.Content.ReadFromJsonAsync<Pipeline>();
            Assert.NotNull(created);
            Assert.Equal("Test Pipeline", created!.Name);
            Assert.True(created.Id > 0);
        }

        [Fact]
        public async Task GetPipelineById_ReturnsNotFoundForMissing()
        {
            var response = await _client.GetAsync("/api/pipelines/99999");
            Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
        }

        [Fact]
        public async Task PostThenGetPipeline_ReturnsSamePipeline()
        {
            var pipeline = new Pipeline
            {
                Name = "Round-trip Pipeline",
                Source = new DataSourceConfig { Name = "s", Type = "csv", ConnectionString = "data.csv" },
                Destination = new DataSourceConfig { Name = "d", Type = "webhook", ConnectionString = "http://example.com" }
            };

            var postResponse = await _client.PostAsJsonAsync("/api/pipelines", pipeline);
            postResponse.EnsureSuccessStatusCode();
            var created = await postResponse.Content.ReadFromJsonAsync<Pipeline>();

            var getResponse = await _client.GetAsync($"/api/pipelines/{created!.Id}");
            Assert.Equal(HttpStatusCode.OK, getResponse.StatusCode);

            var fetched = await getResponse.Content.ReadFromJsonAsync<Pipeline>();
            Assert.NotNull(fetched);
            Assert.Equal("Round-trip Pipeline", fetched!.Name);
        }

        [Fact]
        public async Task PutPipeline_ReturnsNoContent()
        {
            var pipeline = new Pipeline
            {
                Name = "Before Update",
                Source = new DataSourceConfig { Name = "s", Type = "csv", ConnectionString = "f.csv" },
                Destination = new DataSourceConfig { Name = "d", Type = "webhook", ConnectionString = "http://example.com" }
            };

            var postResponse = await _client.PostAsJsonAsync("/api/pipelines", pipeline);
            var created = await postResponse.Content.ReadFromJsonAsync<Pipeline>();

            created!.Name = "After Update";
            var putResponse = await _client.PutAsJsonAsync($"/api/pipelines/{created.Id}", created);
            Assert.Equal(HttpStatusCode.NoContent, putResponse.StatusCode);

            var getResponse = await _client.GetAsync($"/api/pipelines/{created.Id}");
            var updated = await getResponse.Content.ReadFromJsonAsync<Pipeline>();
            Assert.Equal("After Update", updated!.Name);
        }

        [Fact]
        public async Task DeletePipeline_ReturnsNoContent()
        {
            var pipeline = new Pipeline
            {
                Name = "To Delete",
                Source = new DataSourceConfig { Name = "s", Type = "csv", ConnectionString = "f.csv" },
                Destination = new DataSourceConfig { Name = "d", Type = "webhook", ConnectionString = "http://example.com" }
            };

            var postResponse = await _client.PostAsJsonAsync("/api/pipelines", pipeline);
            var created = await postResponse.Content.ReadFromJsonAsync<Pipeline>();

            var deleteResponse = await _client.DeleteAsync($"/api/pipelines/{created!.Id}");
            Assert.Equal(HttpStatusCode.NoContent, deleteResponse.StatusCode);

            var getResponse = await _client.GetAsync($"/api/pipelines/{created.Id}");
            Assert.Equal(HttpStatusCode.NotFound, getResponse.StatusCode);
        }
    }
}
