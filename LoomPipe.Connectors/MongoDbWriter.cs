#nullable enable
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Threading.Tasks;
using LoomPipe.Core.Entities;
using LoomPipe.Core.Exceptions;
using LoomPipe.Core.Interfaces;
using Microsoft.Extensions.Logging;
using MongoDB.Bson;
using MongoDB.Driver;

namespace LoomPipe.Connectors
{
    /// <summary>
    /// Writes documents into a MongoDB collection.
    /// config.ConnectionString = MongoDB connection URI (resolved from ConnectionProfile).
    /// config.Parameters["database"] = database name.
    /// config.Parameters["collection"] = collection name.
    /// </summary>
    public class MongoDbWriter : IDestinationWriter
    {
        private readonly ILogger _logger;

        public MongoDbWriter(ILogger<MongoDbWriter> logger)
        {
            _logger = logger;
        }

        public async Task WriteAsync(DataSourceConfig config, IEnumerable<object> records)
        {
            var collName = GetCollection(config);
            _logger.LogInformation("Writing to MongoDB collection '{Collection}'.", collName);
            try
            {
                var coll = GetMongoCollection(config);
                var docs = records.Select(RecordToBson).ToList();
                if (docs.Count == 0) return;
                await coll.InsertManyAsync(docs);
                _logger.LogInformation("Inserted {Count} documents into MongoDB collection '{Collection}'.", docs.Count, collName);
            }
            catch (Exception ex)
            {
                throw new ConnectorException($"Failed to write to MongoDB: {ex.Message}", ex, "mongodb");
            }
        }

        public Task<bool> ValidateSchemaAsync(DataSourceConfig config, IEnumerable<string> fields)
        {
            // MongoDB is schemaless
            return Task.FromResult(true);
        }

        public Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, IEnumerable<object> records, int sampleSize = 10)
        {
            _logger.LogInformation("Dry run for MongoDB write (no data written).");
            IEnumerable<object> result = records.Take(sampleSize);
            return Task.FromResult(result);
        }

        // ── Helpers ──────────────────────────────────────────────────────────

        private static IMongoCollection<BsonDocument> GetMongoCollection(DataSourceConfig config)
        {
            var client = new MongoClient(config.ConnectionString);
            return client.GetDatabase(GetDatabase(config)).GetCollection<BsonDocument>(GetCollection(config));
        }

        private static string GetDatabase(DataSourceConfig config) =>
            config.Parameters.TryGetValue("database", out var d) ? d?.ToString() ?? "default" : "default";

        private static string GetCollection(DataSourceConfig config) =>
            config.Parameters.TryGetValue("collection", out var c) ? c?.ToString() ?? "data" : "data";

        private static BsonDocument RecordToBson(object record)
        {
            var doc = new BsonDocument();
            if (record is IDictionary<string, object> dict)
                foreach (var kv in dict)
                    doc[kv.Key] = kv.Value == null ? BsonNull.Value : BsonValue.Create(kv.Value);
            return doc;
        }
    }
}
