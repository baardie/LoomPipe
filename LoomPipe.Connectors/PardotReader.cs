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
    /// Reads data from Pardot (Salesforce Marketing Cloud Account Engagement) using the v5 API.
    ///
    /// Parameters:
    ///   accessToken     — Salesforce OAuth Bearer token
    ///   businessUnitId  — Pardot Business Unit ID (Pardot-Business-Unit-Id header)
    ///   resource        — prospects, visitors, visits, campaigns, lists, emails, forms,
    ///                     landing_pages, opportunities, tags, custom_fields, dynamic_content
    /// </summary>
    public class PardotReader : ISourceReader
    {
        private const string BaseUrl = "https://pi.pardot.com/api/v5";
        private const int PageLimit = 200;

        private static readonly string[] AllResources =
        {
            "prospects", "visitors", "visits", "campaigns", "lists", "emails", "forms",
            "landing_pages", "opportunities", "tags", "custom_fields", "dynamic_content"
        };

        private readonly HttpClient _httpClient;
        private readonly ILogger<PardotReader> _logger;

        public PardotReader(HttpClient httpClient, ILogger<PardotReader> logger)
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
            var resource       = GetRequiredParam(config.Parameters, "resource");
            var accessToken    = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Pardot access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "pardot");
            var businessUnitId = GetRequiredParam(config.Parameters, "businessUnitId");

            _logger.LogInformation("Pardot: reading resource '{Resource}'.", resource);

            try
            {
                return await ReadFullAsync(resource, accessToken, businessUnitId);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Pardot: failed to read resource '{Resource}'.", resource);
                throw new ConnectorException($"Failed to read Pardot resource '{resource}': {ex.Message}", ex, "pardot");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var resource       = GetRequiredParam(config.Parameters, "resource");
            var accessToken    = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Pardot access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "pardot");
            var businessUnitId = GetRequiredParam(config.Parameters, "businessUnitId");

            _logger.LogInformation("Pardot: discovering schema for '{Resource}'.", resource);

            try
            {
                var sample = await ReadFullAsync(resource, accessToken, businessUnitId, maxPages: 1);
                var first = sample.FirstOrDefault() as IDictionary<string, object>;
                return first?.Keys ?? Array.Empty<string>();
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Pardot: failed to discover schema for '{Resource}'.", resource);
                throw new ConnectorException($"Failed to discover Pardot schema for '{resource}': {ex.Message}", ex, "pardot");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var resource       = GetRequiredParam(config.Parameters, "resource");
            var accessToken    = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Pardot access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "pardot");
            var businessUnitId = GetRequiredParam(config.Parameters, "businessUnitId");

            _logger.LogInformation("Pardot: dry run preview for '{Resource}' (sample={SampleSize}).", resource, sampleSize);

            try
            {
                var records = await ReadFullAsync(resource, accessToken, businessUnitId, maxPages: 1);
                return records.Take(sampleSize);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Pardot: dry run preview failed for '{Resource}'.", resource);
                throw new ConnectorException($"Pardot dry run preview failed for '{resource}': {ex.Message}", ex, "pardot");
            }
        }

        public Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            return Task.FromResult<IEnumerable<string>>(AllResources);
        }

        // ── Full read (paginated GET) ────────────────────────────────────────

        private async Task<List<object>> ReadFullAsync(
            string resource, string accessToken, string businessUnitId, int maxPages = int.MaxValue)
        {
            var results = new List<object>();
            string? afterCursor = null;
            int page = 0;

            do
            {
                var url = BuildUrl(resource, afterCursor);

                using var request = new HttpRequestMessage(HttpMethod.Get, url);
                ApplyAuth(request, accessToken, businessUnitId);

                using var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);

                afterCursor = ParseResultsPage(doc.RootElement, results);

                page++;
            }
            while (afterCursor != null && page < maxPages);

            _logger.LogInformation("Pardot: read {Count} records from '{Resource}' across {Pages} page(s).",
                results.Count, resource, page);

            return results;
        }

        // ── URL builder ─────────────────────────────────────────────────────

        private static string BuildUrl(string resource, string? afterCursor)
        {
            var endpoint = GetEndpoint(resource);
            var sb = new StringBuilder();

            sb.Append($"{BaseUrl}/objects/{endpoint}");
            sb.Append($"?fields={GetDefaultFields(resource)}");
            sb.Append($"&limit={PageLimit}");

            if (!string.IsNullOrEmpty(afterCursor))
            {
                sb.Append($"&after={Uri.EscapeDataString(afterCursor)}");
            }

            return sb.ToString();
        }

        /// <summary>
        /// Maps a resource name to the Pardot v5 API endpoint path segment.
        /// </summary>
        private static string GetEndpoint(string resource) => resource switch
        {
            "prospects"       => "prospects",
            "visitors"        => "visitors",
            "visits"          => "visits",
            "campaigns"       => "campaigns",
            "lists"           => "lists",
            "emails"          => "emails",
            "forms"           => "forms",
            "landing_pages"   => "landing-pages",
            "opportunities"   => "opportunities",
            "tags"            => "tags",
            "custom_fields"   => "custom-fields",
            "dynamic_content" => "dynamic-content",
            _ => throw new ConnectorException(
                $"Pardot: unsupported resource '{resource}'. Supported: {string.Join(", ", AllResources)}",
                new ArgumentException($"Unsupported resource: {resource}"),
                "pardot")
        };

        /// <summary>
        /// Returns default field names for each resource type.
        /// </summary>
        private static string GetDefaultFields(string resource) => resource switch
        {
            "prospects"       => "id,email,firstName,lastName,company,createdAt,updatedAt",
            "visitors"        => "id,pageViewCount,ipAddress,hostname,createdAt,updatedAt",
            "visits"          => "id,visitorId,prospectId,createdAt,durationInSeconds,campaignId",
            "campaigns"       => "id,name,cost,createdAt,updatedAt",
            "lists"           => "id,name,title,description,isPublic,isDynamic,createdAt,updatedAt",
            "emails"          => "id,name,subject,createdAt,updatedAt",
            "forms"           => "id,name,campaignId,createdAt,updatedAt",
            "landing_pages"   => "id,name,url,campaignId,createdAt,updatedAt",
            "opportunities"   => "id,name,value,probability,status,createdAt,updatedAt",
            "tags"            => "id,name,createdAt,updatedAt",
            "custom_fields"   => "id,name,fieldId,type,createdAt,updatedAt",
            "dynamic_content" => "id,name,createdAt,updatedAt",
            _                 => "id"
        };

        // ── Response parsing ─────────────────────────────────────────────────

        /// <summary>
        /// Parses a Pardot v5 response: { "values": [...], "nextPageToken": "..." }.
        /// Returns the nextPageToken for pagination, or null.
        /// </summary>
        private static string? ParseResultsPage(JsonElement root, List<object> results)
        {
            string? nextPageToken = null;

            if (root.TryGetProperty("nextPageToken", out var npt)
                && npt.ValueKind == JsonValueKind.String)
            {
                nextPageToken = npt.GetString();
            }

            JsonElement items;

            if (root.TryGetProperty("values", out items) && items.ValueKind == JsonValueKind.Array)
            {
                // Standard Pardot v5 response shape
            }
            else if (root.ValueKind == JsonValueKind.Array)
            {
                items = root;
            }
            else
            {
                return nextPageToken;
            }

            foreach (var element in items.EnumerateArray())
            {
                IDictionary<string, object> row = new ExpandoObject();

                foreach (var prop in element.EnumerateObject())
                {
                    row[prop.Name] = ConvertJsonValue(prop.Value);
                }

                results.Add(row);
            }

            return nextPageToken;
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

        private static void ApplyAuth(HttpRequestMessage request, string accessToken, string businessUnitId)
        {
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", accessToken);
            request.Headers.Add("Pardot-Business-Unit-Id", businessUnitId);
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
                    $"Pardot connector requires the '{key}' parameter.",
                    new ArgumentException($"Missing required parameter: {key}"),
                    "pardot");
            return value;
        }
    }
}
