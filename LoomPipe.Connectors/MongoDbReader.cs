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
    /// Reads documents from a MongoDB collection.
    /// config.ConnectionString = MongoDB connection URI (resolved from ConnectionProfile).
    /// config.Parameters["database"] = database name.
    /// config.Parameters["collection"] = collection name.
    /// </summary>
    public class MongoDbReader : ISourceReader
    {
        private readonly ILogger _logger;

        public MongoDbReader(ILogger<MongoDbReader> logger)
        {
            _logger = logger;
        }

        public async Task<IEnumerable<object>> ReadAsync(DataSourceConfig config)
        {
            _logger.LogInformation("Reading from MongoDB collection '{Collection}'.", GetCollection(config));
            try
            {
                var coll = GetMongoCollection(config);
                var docs = await coll.FindAsync(FilterDefinition<BsonDocument>.Empty);
                return (await docs.ToListAsync()).Select(BsonToExpando).ToList<object>();
            }
            catch (Exception ex)
            {
                throw new ConnectorException("Failed to read from MongoDB. See inner exception.", ex);
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            _logger.LogInformation("Discovering schema for MongoDB collection '{Collection}'.", GetCollection(config));
            try
            {
                var coll = GetMongoCollection(config);
                var cursor = await coll.FindAsync(FilterDefinition<BsonDocument>.Empty,
                    new FindOptions<BsonDocument> { Limit = 1 });
                var first = await cursor.FirstOrDefaultAsync();
                return first?.Names ?? Array.Empty<string>();
            }
            catch (Exception ex)
            {
                throw new ConnectorException("Failed to discover MongoDB schema. See inner exception.", ex);
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            _logger.LogInformation("Dry run preview from MongoDB collection '{Collection}'.", GetCollection(config));
            try
            {
                var coll = GetMongoCollection(config);
                var cursor = await coll.FindAsync(FilterDefinition<BsonDocument>.Empty,
                    new FindOptions<BsonDocument> { Limit = sampleSize });
                var docs = await cursor.ToListAsync();
                return docs.Select(BsonToExpando).ToList<object>();
            }
            catch (Exception ex)
            {
                throw new ConnectorException("MongoDB dry run failed. See inner exception.", ex);
            }
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

        private static object BsonToExpando(BsonDocument doc)
        {
            IDictionary<string, object> expando = new ExpandoObject();
            foreach (var elem in doc)
                expando[elem.Name] = BsonValueToObject(elem.Value);
            return expando;
        }

        private static object BsonValueToObject(BsonValue v) => v.BsonType switch
        {
            BsonType.String   => v.AsString,
            BsonType.Int32    => v.AsInt32,
            BsonType.Int64    => v.AsInt64,
            BsonType.Double   => v.AsDouble,
            BsonType.Boolean  => v.AsBoolean,
            BsonType.ObjectId => v.AsObjectId.ToString(),
            _                 => v.ToString() ?? string.Empty,
        };
    }
}
