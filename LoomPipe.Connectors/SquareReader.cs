#nullable enable
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using LoomPipe.Core.Entities;
using LoomPipe.Core.Exceptions;
using LoomPipe.Core.Interfaces;
using Microsoft.Extensions.Logging;

namespace LoomPipe.Connectors
{
    /// <summary>
    /// Reads data from the Square API v2.
    ///
    /// Parameters:
    ///   accessToken  — Square access token (OAuth or personal)
    ///   resource     — payments, orders, customers, invoices, catalog, inventory,
    ///                  locations, employees, team_members, bookings
    ///   locationId   — required for payments, orders, and inventory
    /// </summary>
    public class SquareReader : ISourceReader
    {
        private const string BaseUrl = "https://connect.squareup.com/v2";
        private const int PageLimit = 100;

        private static readonly string[] AllResources =
        {
            "payments", "orders", "customers", "invoices", "catalog",
            "inventory", "locations", "employees", "team_members", "bookings"
        };

        private readonly HttpClient _httpClient;
        private readonly ILogger<SquareReader> _logger;

        public SquareReader(HttpClient httpClient, ILogger<SquareReader> logger)
        {
            _httpClient = httpClient;
            _logger = logger;
        }

        // ── ISourceReader ────────────────────────────────────────────────────

        public async Task<IEnumerable<object>> ReadAsync(
            DataSourceConfig config,
            string? watermarkField = null,
            string? watermarkValue = null)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Square access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "square");

            _logger.LogInformation("Square: reading resource '{Resource}'.", resource);

            try
            {
                return await ReadFullAsync(resource, accessToken, config.Parameters);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Square: failed to read resource '{Resource}'.", resource);
                throw new ConnectorException($"Failed to read Square resource '{resource}': {ex.Message}", ex, "square");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Square access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "square");

            _logger.LogInformation("Square: discovering schema for '{Resource}'.", resource);

            try
            {
                var sample = await ReadFullAsync(resource, accessToken, config.Parameters, maxPages: 1);
                var first = sample.FirstOrDefault() as IDictionary<string, object>;
                return first?.Keys ?? Array.Empty<string>();
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Square: failed to discover schema for '{Resource}'.", resource);
                throw new ConnectorException($"Failed to discover Square schema for '{resource}': {ex.Message}", ex, "square");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Square access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "square");

            _logger.LogInformation("Square: dry run preview for '{Resource}' (sample={SampleSize}).", resource, sampleSize);

            try
            {
                var records = await ReadFullAsync(resource, accessToken, config.Parameters, maxPages: 1);
                return records.Take(sampleSize);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Square: dry run preview failed for '{Resource}'.", resource);
                throw new ConnectorException($"Square dry run preview failed for '{resource}': {ex.Message}", ex, "square");
            }
        }

        public Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            return Task.FromResult<IEnumerable<string>>(AllResources);
        }

        // ── Full read (cursor-based pagination) ──────────────────────────────

        private async Task<List<object>> ReadFullAsync(
            string resource, string accessToken, Dictionary<string, object> parameters, int maxPages = int.MaxValue)
        {
            var results = new List<object>();
            string? cursor = null;
            int page = 0;

            // Orders and search-based endpoints use POST; others use GET.
            var usePost = resource is "orders";

            do
            {
                (List<object> pageResults, string? nextCursor) = usePost
                    ? await ReadPostPageAsync(resource, accessToken, parameters, cursor)
                    : await ReadGetPageAsync(resource, accessToken, parameters, cursor);

                results.AddRange(pageResults);
                cursor = nextCursor;
                page++;
            }
            while (cursor != null && page < maxPages);

            _logger.LogInformation("Square: read {Count} records from '{Resource}' across {Pages} page(s).",
                results.Count, resource, page);

            return results;
        }

        // ── GET-based page read ──────────────────────────────────────────────

        private async Task<(List<object> Results, string? NextCursor)> ReadGetPageAsync(
            string resource, string accessToken, Dictionary<string, object> parameters,
            string? cursor)
        {
            var url = BuildGetUrl(resource, parameters, cursor);

            using var request = new HttpRequestMessage(HttpMethod.Get, url);
            ApplyAuth(request, accessToken);

            using var response = await _httpClient.SendAsync(request);
            response.EnsureSuccessStatusCode();

            var json = await response.Content.ReadAsStringAsync();
            using var doc = JsonDocument.Parse(json);

            var resourceKey = GetResourceKey(resource);
            var pageResults = new List<object>();
            ParseResultsPage(doc.RootElement, resourceKey, pageResults);

            // Cursor pagination
            string? nextCursor = null;
            if (doc.RootElement.TryGetProperty("cursor", out var cursorEl)
                && cursorEl.ValueKind == JsonValueKind.String)
            {
                nextCursor = cursorEl.GetString();
            }

            return (pageResults, nextCursor);
        }

        // ── POST-based page read (orders search) ────────────────────────────

        private async Task<(List<object> Results, string? NextCursor)> ReadPostPageAsync(
            string resource, string accessToken, Dictionary<string, object> parameters,
            string? cursor)
        {
            var locationId = GetStringParam(parameters, "locationId");
            var url = $"{BaseUrl}/orders/search";

            using var ms = new System.IO.MemoryStream();
            using var writer = new Utf8JsonWriter(ms);
            writer.WriteStartObject();

            if (!string.IsNullOrEmpty(locationId))
            {
                writer.WritePropertyName("location_ids");
                writer.WriteStartArray();
                writer.WriteStringValue(locationId);
                writer.WriteEndArray();
            }

            writer.WriteNumber("limit", PageLimit);

            if (!string.IsNullOrEmpty(cursor))
                writer.WriteString("cursor", cursor);

            writer.WriteEndObject();
            writer.Flush();

            var body = Encoding.UTF8.GetString(ms.ToArray());

            using var request = new HttpRequestMessage(HttpMethod.Post, url)
            {
                Content = new StringContent(body, Encoding.UTF8, "application/json")
            };
            ApplyAuth(request, accessToken);

            using var response = await _httpClient.SendAsync(request);
            response.EnsureSuccessStatusCode();

            var json = await response.Content.ReadAsStringAsync();
            using var doc = JsonDocument.Parse(json);

            var pageResults = new List<object>();
            ParseResultsPage(doc.RootElement, "orders", pageResults);

            string? nextCursor = null;
            if (doc.RootElement.TryGetProperty("cursor", out var cursorEl)
                && cursorEl.ValueKind == JsonValueKind.String)
            {
                nextCursor = cursorEl.GetString();
            }

            return (pageResults, nextCursor);
        }

        // ── URL builder ──────────────────────────────────────────────────────

        private static string BuildGetUrl(string resource, Dictionary<string, object> parameters, string? cursor)
        {
            var locationId = GetStringParam(parameters, "locationId");

            var sb = new StringBuilder();

            var endpoint = resource switch
            {
                "payments"     => "/payments",
                "customers"    => "/customers",
                "invoices"     => "/invoices",
                "catalog"      => "/catalog/list",
                "inventory"    => "/inventory/changes",
                "locations"    => "/locations",
                "employees"    => "/employees",
                "team_members" => "/team-members/search",
                "bookings"     => "/bookings",
                _ => throw new ConnectorException(
                    $"Square: unsupported resource '{resource}'.",
                    new ArgumentException($"Unsupported resource: {resource}"),
                    "square")
            };

            sb.Append(BaseUrl);
            sb.Append(endpoint);
            sb.Append('?');

            // Add location_id where required.
            if (!string.IsNullOrEmpty(locationId) && resource is "payments" or "invoices" or "inventory")
            {
                sb.Append($"location_id={Uri.EscapeDataString(locationId)}&");
            }

            sb.Append($"limit={PageLimit}");

            if (!string.IsNullOrEmpty(cursor))
            {
                sb.Append($"&cursor={Uri.EscapeDataString(cursor)}");
            }

            return sb.ToString();
        }

        /// <summary>
        /// Returns the JSON property name that wraps the results array for a given resource.
        /// </summary>
        private static string GetResourceKey(string resource) => resource switch
        {
            "payments"     => "payments",
            "orders"       => "orders",
            "customers"    => "customers",
            "invoices"     => "invoices",
            "catalog"      => "objects",
            "inventory"    => "changes",
            "locations"    => "locations",
            "employees"    => "employees",
            "team_members" => "team_members",
            "bookings"     => "bookings",
            _              => resource
        };

        // ── Response parsing ─────────────────────────────────────────────────

        private static void ParseResultsPage(JsonElement root, string resourceKey, List<object> results)
        {
            JsonElement items;

            if (root.TryGetProperty(resourceKey, out items) && items.ValueKind == JsonValueKind.Array)
            {
                // Standard response shape
            }
            else if (root.ValueKind == JsonValueKind.Array)
            {
                items = root;
            }
            else
            {
                return;
            }

            foreach (var element in items.EnumerateArray())
            {
                IDictionary<string, object> row = new ExpandoObject();

                foreach (var prop in element.EnumerateObject())
                {
                    if (prop.Value.ValueKind == JsonValueKind.Object)
                    {
                        // Flatten one level of nested objects (e.g. "amount_money").
                        foreach (var nested in prop.Value.EnumerateObject())
                        {
                            if (nested.Value.ValueKind == JsonValueKind.Object || nested.Value.ValueKind == JsonValueKind.Array)
                                row[$"{prop.Name}_{nested.Name}"] = nested.Value.ToString();
                            else
                                row[$"{prop.Name}_{nested.Name}"] = ConvertJsonValue(nested.Value);
                        }
                    }
                    else if (prop.Value.ValueKind == JsonValueKind.Array)
                    {
                        row[prop.Name] = prop.Value.ToString();
                    }
                    else
                    {
                        row[prop.Name] = ConvertJsonValue(prop.Value);
                    }
                }

                results.Add(row);
            }
        }

        private static object ConvertJsonValue(JsonElement value)
        {
            return value.ValueKind switch
            {
                JsonValueKind.String  => (object)(value.GetString() ?? string.Empty),
                JsonValueKind.Number  => value.TryGetInt64(out var l) ? l : value.GetDouble(),
                JsonValueKind.True    => true,
                JsonValueKind.False   => false,
                JsonValueKind.Null    => string.Empty,
                _                     => value.ToString()
            };
        }

        // ── Auth helper ──────────────────────────────────────────────────────

        private static void ApplyAuth(HttpRequestMessage request, string accessToken)
        {
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", accessToken);
        }

        // ── Parameter helpers ────────────────────────────────────────────────

        private static string? GetStringParam(Dictionary<string, object> p, string key)
        {
            if (!p.TryGetValue(key, out var v)) return null;
            if (v is JsonElement je)
                return je.ValueKind == JsonValueKind.String ? je.GetString() : je.ToString();
            return v?.ToString();
        }

        private static string GetRequiredParam(Dictionary<string, object> p, string key)
        {
            var value = GetStringParam(p, key);
            if (string.IsNullOrWhiteSpace(value))
                throw new ConnectorException(
                    $"Square connector requires the '{key}' parameter.",
                    new ArgumentException($"Missing required parameter: {key}"),
                    "square");
            return value;
        }
    }
}
