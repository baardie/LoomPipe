#nullable enable
using System;
using System.Data.Common;
using System.Diagnostics;
using System.Text.Json;
using System.Threading.Tasks;
using LoomPipe.Core.DTOs;
using LoomPipe.Core.Interfaces;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using MySqlConnector;
using Neo4j.Driver;
using Npgsql;
using Oracle.ManagedDataAccess.Client;
using Pinecone;
using Snowflake.Data.Client;

namespace LoomPipe.Connectors
{
    public class ConnectorFactory : IConnectorFactory
    {
        private readonly IHttpClientFactory _httpClientFactory;
        private readonly ILoggerFactory _loggerFactory;

        public ConnectorFactory(IHttpClientFactory httpClientFactory, ILoggerFactory loggerFactory)
        {
            _httpClientFactory = httpClientFactory;
            _loggerFactory = loggerFactory;
        }

        // ── Source Readers ────────────────────────────────────────────────────

        public ISourceReader CreateSourceReader(string type) => type.ToLowerInvariant() switch
        {
            "csv"        => new CsvSourceReader(_loggerFactory.CreateLogger<CsvSourceReader>()),
            "rest"       => new RestSourceReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<RestSourceReader>()),
            "sqlserver"  => new RelationalDbReader("sqlserver",  _loggerFactory.CreateLogger<RelationalDbReader>()),
            "postgresql" => new RelationalDbReader("postgresql", _loggerFactory.CreateLogger<RelationalDbReader>()),
            "mysql"      => new RelationalDbReader("mysql",      _loggerFactory.CreateLogger<RelationalDbReader>()),
            "oracle"     => new RelationalDbReader("oracle",     _loggerFactory.CreateLogger<RelationalDbReader>()),
            "mongodb"    => new MongoDbReader(_loggerFactory.CreateLogger<MongoDbReader>()),
            "neo4j"      => new Neo4jReader(_loggerFactory.CreateLogger<Neo4jReader>()),
            "snowflake"  => new SnowflakeReader(_loggerFactory.CreateLogger<SnowflakeReader>()),
            "bigquery"   => new BigQueryReader(_loggerFactory.CreateLogger<BigQueryReader>()),
            "pinecone"   => new PineconeReader(_loggerFactory.CreateLogger<PineconeReader>()),
            "milvus"     => new MilvusReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<MilvusReader>()),
            _            => throw new NotSupportedException($"Source type '{type}' is not supported.")
        };

        // ── Destination Writers ───────────────────────────────────────────────

        public IDestinationWriter CreateDestinationWriter(string type) => type.ToLowerInvariant() switch
        {
            "webhook"    => new WebhookDestinationWriter(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<WebhookDestinationWriter>()),
            "sqlserver"  => new RelationalDbWriter("sqlserver",  _loggerFactory.CreateLogger<RelationalDbWriter>()),
            "postgresql" => new RelationalDbWriter("postgresql", _loggerFactory.CreateLogger<RelationalDbWriter>()),
            "mysql"      => new RelationalDbWriter("mysql",      _loggerFactory.CreateLogger<RelationalDbWriter>()),
            "oracle"     => new RelationalDbWriter("oracle",     _loggerFactory.CreateLogger<RelationalDbWriter>()),
            "mongodb"    => new MongoDbWriter(_loggerFactory.CreateLogger<MongoDbWriter>()),
            "neo4j"      => new Neo4jWriter(_loggerFactory.CreateLogger<Neo4jWriter>()),
            "snowflake"  => new SnowflakeWriter(_loggerFactory.CreateLogger<SnowflakeWriter>()),
            "bigquery"   => new BigQueryWriter(_loggerFactory.CreateLogger<BigQueryWriter>()),
            "pinecone"   => new PineconeWriter(_loggerFactory.CreateLogger<PineconeWriter>()),
            "milvus"     => new MilvusWriter(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<MilvusWriter>()),
            _            => throw new NotSupportedException($"Destination type '{type}' is not supported.")
        };

        // ── Connection Test ───────────────────────────────────────────────────

        public async Task<ConnectionTestResult> TestConnectionAsync(string provider, string connectionString)
        {
            var sw = Stopwatch.StartNew();
            try
            {
                await OpenAndCloseAsync(provider, connectionString);
                sw.Stop();
                return new ConnectionTestResult { Success = true, ElapsedMs = sw.ElapsedMilliseconds };
            }
            catch (Exception ex)
            {
                sw.Stop();
                return new ConnectionTestResult
                {
                    Success      = false,
                    ErrorMessage = ex.Message,
                    ElapsedMs    = sw.ElapsedMilliseconds,
                };
            }
        }

        private static async Task OpenAndCloseAsync(string provider, string connectionString)
        {
            switch (provider.ToLowerInvariant())
            {
                case "sqlserver":
                    await using (var conn = new SqlConnection(connectionString)) { await conn.OpenAsync(); }
                    break;
                case "postgresql":
                    await using (var conn = new NpgsqlConnection(connectionString)) { await conn.OpenAsync(); }
                    break;
                case "mysql":
                    await using (var conn = new MySqlConnection(connectionString)) { await conn.OpenAsync(); }
                    break;
                case "oracle":
                    await using (var conn = new OracleConnection(connectionString)) { await conn.OpenAsync(); }
                    break;
                case "snowflake":
                    await using (var conn = new SnowflakeDbConnection())
                    {
                        conn.ConnectionString = connectionString;
                        await conn.OpenAsync();
                    }
                    break;
                case "mongodb":
                    var mongoClient = new MongoDB.Driver.MongoClient(connectionString);
                    await mongoClient.ListDatabaseNamesAsync();
                    break;
                case "neo4j":
                {
                    var opts = JsonSerializer.Deserialize<Neo4jTestOpts>(connectionString)
                               ?? throw new InvalidOperationException("Invalid Neo4j connection string.");
                    await using var driver = GraphDatabase.Driver(opts.Uri, AuthTokens.Basic(opts.User, opts.Password));
                    await driver.VerifyConnectivityAsync();
                    break;
                }
                case "bigquery":
                {
                    var opts = JsonSerializer.Deserialize<BqTestOpts>(connectionString)
                               ?? throw new InvalidOperationException("Invalid BigQuery connection string.");
                    var credential = Google.Apis.Auth.OAuth2.GoogleCredential.FromJson(opts.ServiceAccountJson)
                        .CreateScoped("https://www.googleapis.com/auth/bigquery");
                    var client = Google.Cloud.BigQuery.V2.BigQueryClient.Create(opts.ProjectId, credential);
                    await client.GetDatasetAsync(opts.Dataset);
                    break;
                }
                case "pinecone":
                {
                    var opts = JsonSerializer.Deserialize<PcTestOpts>(connectionString)
                               ?? throw new InvalidOperationException("Invalid Pinecone connection string.");
                    var client = new PineconeClient(opts.ApiKey);
                    await client.ListIndexesAsync();
                    break;
                }
                case "milvus":
                {
                    var opts = JsonSerializer.Deserialize<MilvusTestOpts>(connectionString)
                               ?? throw new InvalidOperationException("Invalid Milvus connection string.");
                    using var http = new System.Net.Http.HttpClient();
                    var resp = await http.GetAsync($"http://{opts.Host}:{opts.Port}/healthz");
                    resp.EnsureSuccessStatusCode();
                    break;
                }
                default:
                    throw new NotSupportedException($"Provider '{provider}' is not supported for connection testing.");
            }
        }

        // Private records for JSON deserialization in TestConnectionAsync
        private record Neo4jTestOpts(string Uri, string User, string Password);
        private record BqTestOpts(string ProjectId, string Dataset, string ServiceAccountJson);
        private record PcTestOpts(string ApiKey, string IndexName, string Environment);
        private record MilvusTestOpts(string Host, int Port, string Collection, string User, string Password);
    }
}
