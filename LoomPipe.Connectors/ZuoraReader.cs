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
    /// Reads data from Zuora using the REST API v1.
    ///
    /// Parameters:
    ///   accessToken  — Bearer token (OAuth) or API credentials
    ///   resource     — accounts, subscriptions, invoices, payments, products, rate_plans,
    ///                  usage, amendments, contacts, credit_balance_adjustments
    ///   query        — optional ZOQL query string (overrides default SELECT * FROM {resource})
    /// </summary>
    public class ZuoraReader : ISourceReader
    {
        private const string BaseUrl = "https://rest.zuora.com/v1";

        private static readonly string[] AllResources =
        {
            "accounts", "subscriptions", "invoices", "payments", "products",
            "rate_plans", "usage", "amendments", "contacts", "credit_balance_adjustments"
        };

        private readonly HttpClient _httpClient;
        private readonly ILogger<ZuoraReader> _logger;

        public ZuoraReader(HttpClient httpClient, ILogger<ZuoraReader> logger)
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
                    "Zuora access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "zuora");
            var query       = GetStringParam(config.Parameters, "query");

            _logger.LogInformation("Zuora: reading resource '{Resource}'.", resource);

            try
            {
                var zoql = query ?? $"SELECT * FROM {NormalizeObjectType(resource)}";

                if (!string.IsNullOrEmpty(watermarkField) && !string.IsNullOrEmpty(watermarkValue))
                {
                    zoql = query ?? $"SELECT * FROM {NormalizeObjectType(resource)} WHERE {watermarkField} > '{watermarkValue}'";
                }

                return await QueryAsync(zoql, accessToken);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Zuora: failed to read resource '{Resource}'.", resource);
                throw new ConnectorException($"Failed to read Zuora resource '{resource}': {ex.Message}", ex, "zuora");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Zuora access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "zuora");

            _logger.LogInformation("Zuora: discovering schema for '{Resource}'.", resource);

            try
            {
                var zoql = $"SELECT * FROM {NormalizeObjectType(resource)}";
                var sample = await QueryAsync(zoql, accessToken, maxPages: 1);
                var first = sample.FirstOrDefault() as IDictionary<string, object>;
                return first?.Keys ?? Array.Empty<string>();
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Zuora: failed to discover schema for '{Resource}'.", resource);
                throw new ConnectorException($"Failed to discover Zuora schema for '{resource}': {ex.Message}", ex, "zuora");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Zuora access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "zuora");

            _logger.LogInformation("Zuora: dry run preview for '{Resource}' (sample={SampleSize}).", resource, sampleSize);

            try
            {
                var zoql = $"SELECT * FROM {NormalizeObjectType(resource)}";
                var records = await QueryAsync(zoql, accessToken, maxPages: 1);
                return records.Take(sampleSize);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Zuora: dry run preview failed for '{Resource}'.", resource);
                throw new ConnectorException($"Zuora dry run preview failed for '{resource}': {ex.Message}", ex, "zuora");
            }
        }

        public Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            return Task.FromResult<IEnumerable<string>>(AllResources);
        }

        // ── ZOQL query with queryLocator pagination ──────────────────────────

        private async Task<List<object>> QueryAsync(
            string zoql, string accessToken, int maxPages = int.MaxValue)
        {
            var results = new List<object>();
            int page = 0;

            // Initial query
            var queryBody = BuildQueryBody(zoql);

            using var request = new HttpRequestMessage(HttpMethod.Post, $"{BaseUrl}/action/query")
            {
                Content = new StringContent(queryBody, Encoding.UTF8, "application/json")
            };
            ApplyAuth(request, accessToken);

            using var response = await _httpClient.SendAsync(request);
            response.EnsureSuccessStatusCode();

            var json = await response.Content.ReadAsStringAsync();
            using var doc = JsonDocument.Parse(json);

            ParseQueryResults(doc.RootElement, results);
            page++;

            // Follow queryLocator for subsequent pages
            bool done = true;
            string? queryLocator = null;

            if (doc.RootElement.TryGetProperty("done", out var doneEl))
                done = doneEl.GetBoolean();

            if (doc.RootElement.TryGetProperty("queryLocator", out var locatorEl)
                && locatorEl.ValueKind == JsonValueKind.String)
            {
                queryLocator = locatorEl.GetString();
            }

            while (!done && queryLocator != null && page < maxPages)
            {
                var moreBody = BuildQueryMoreBody(queryLocator);

                using var moreRequest = new HttpRequestMessage(HttpMethod.Post, $"{BaseUrl}/action/queryMore")
                {
                    Content = new StringContent(moreBody, Encoding.UTF8, "application/json")
                };
                ApplyAuth(moreRequest, accessToken);

                using var moreResponse = await _httpClient.SendAsync(moreRequest);
                moreResponse.EnsureSuccessStatusCode();

                var moreJson = await moreResponse.Content.ReadAsStringAsync();
                using var moreDoc = JsonDocument.Parse(moreJson);

                ParseQueryResults(moreDoc.RootElement, results);

                done = true;
                queryLocator = null;

                if (moreDoc.RootElement.TryGetProperty("done", out var moreDoneEl))
                    done = moreDoneEl.GetBoolean();

                if (moreDoc.RootElement.TryGetProperty("queryLocator", out var moreLocatorEl)
                    && moreLocatorEl.ValueKind == JsonValueKind.String)
                {
                    queryLocator = moreLocatorEl.GetString();
                }

                page++;
            }

            _logger.LogInformation("Zuora: query returned {Count} records across {Pages} page(s).",
                results.Count, page);

            return results;
        }

        // ── Request body builders ────────────────────────────────────────────

        private static string BuildQueryBody(string zoql)
        {
            using var ms = new System.IO.MemoryStream();
            using var writer = new Utf8JsonWriter(ms);

            writer.WriteStartObject();
            writer.WriteString("queryString", zoql);
            writer.WriteEndObject();
            writer.Flush();

            return Encoding.UTF8.GetString(ms.ToArray());
        }

        private static string BuildQueryMoreBody(string queryLocator)
        {
            using var ms = new System.IO.MemoryStream();
            using var writer = new Utf8JsonWriter(ms);

            writer.WriteStartObject();
            writer.WriteString("queryLocator", queryLocator);
            writer.WriteEndObject();
            writer.Flush();

            return Encoding.UTF8.GetString(ms.ToArray());
        }

        // ── Response parsing ─────────────────────────────────────────────────

        private static void ParseQueryResults(JsonElement root, List<object> results)
        {
            if (!root.TryGetProperty("records", out var records) || records.ValueKind != JsonValueKind.Array)
                return;

            foreach (var element in records.EnumerateArray())
            {
                IDictionary<string, object> row = new ExpandoObject();

                foreach (var prop in element.EnumerateObject())
                {
                    row[prop.Name] = ConvertJsonValue(prop.Value);
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

        // ── Object-type normalisation ────────────────────────────────────────

        /// <summary>
        /// Normalises the resource name into the Zuora ZOQL object name.
        /// </summary>
        private static string NormalizeObjectType(string resource) => resource switch
        {
            "accounts"                     => "Account",
            "subscriptions"                => "Subscription",
            "invoices"                     => "Invoice",
            "payments"                     => "Payment",
            "products"                     => "Product",
            "rate_plans"                   => "RatePlan",
            "usage"                        => "Usage",
            "amendments"                   => "Amendment",
            "contacts"                     => "Contact",
            "credit_balance_adjustments"   => "CreditBalanceAdjustment",
            _                              => resource
        };

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
                    $"Zuora connector requires the '{key}' parameter.",
                    new ArgumentException($"Missing required parameter: {key}"),
                    "zuora");
            return value;
        }
    }
}
