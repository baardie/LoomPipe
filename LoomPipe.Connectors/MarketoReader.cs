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
    /// Reads data from Marketo using the REST API.
    ///
    /// Parameters:
    ///   clientId      — Marketo API client ID
    ///   clientSecret  — Marketo API client secret
    ///   munchkinId    — Marketo Munchkin ID (e.g. "123-ABC-456")
    ///   resource      — leads, activities, campaigns, programs, lists, smart_lists,
    ///                   smart_campaigns, emails, landing_pages, forms, tokens, channels
    ///
    /// ConnectionString JSON: {"munchkinId":"...","clientId":"...","clientSecret":"..."}
    /// Auth: Bearer token obtained via client_credentials grant at /identity/oauth/token
    /// Pagination: nextPageToken
    /// </summary>
    public class MarketoReader : ISourceReader
    {
        private const int BatchSize = 300;

        private static readonly string[] AllResources =
        {
            "leads", "activities", "campaigns", "programs", "lists",
            "smart_lists", "smart_campaigns", "emails", "landing_pages",
            "forms", "tokens", "channels"
        };

        private readonly HttpClient _httpClient;
        private readonly ILogger<MarketoReader> _logger;

        public MarketoReader(HttpClient httpClient, ILogger<MarketoReader> logger)
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
            var resource = GetRequiredParam(config.Parameters, "resource");
            var (munchkinId, clientId, clientSecret) = ResolveCredentials(config);
            var baseUrl = $"https://{munchkinId}.mktorest.com";

            _logger.LogInformation("Marketo: reading resource '{Resource}'.", resource);

            try
            {
                var accessToken = await ObtainAccessTokenAsync(baseUrl, clientId, clientSecret);
                return await ReadPaginatedAsync(baseUrl, accessToken, resource);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Marketo: failed to read resource '{Resource}'.", resource);
                throw new ConnectorException($"Failed to read Marketo resource '{resource}': {ex.Message}", ex, "marketo");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var resource = GetRequiredParam(config.Parameters, "resource");

            _logger.LogInformation("Marketo: discovering schema for '{Resource}'.", resource);

            try
            {
                var (munchkinId, clientId, clientSecret) = ResolveCredentials(config);
                var baseUrl = $"https://{munchkinId}.mktorest.com";
                var accessToken = await ObtainAccessTokenAsync(baseUrl, clientId, clientSecret);

                // For leads, use the describe endpoint.
                if (resource == "leads")
                {
                    return await DescribeLeadsAsync(baseUrl, accessToken);
                }

                // For other resources, read a small sample and derive field names.
                var sample = (await ReadPaginatedAsync(baseUrl, accessToken, resource, maxPages: 1)).ToList();
                var first = sample.FirstOrDefault() as IDictionary<string, object>;
                return first?.Keys ?? Array.Empty<string>();
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Marketo: failed to discover schema for '{Resource}'.", resource);
                throw new ConnectorException($"Failed to discover Marketo schema for '{resource}': {ex.Message}", ex, "marketo");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var resource = GetRequiredParam(config.Parameters, "resource");

            _logger.LogInformation("Marketo: dry run preview for '{Resource}' (sample={SampleSize}).", resource, sampleSize);

            try
            {
                var (munchkinId, clientId, clientSecret) = ResolveCredentials(config);
                var baseUrl = $"https://{munchkinId}.mktorest.com";
                var accessToken = await ObtainAccessTokenAsync(baseUrl, clientId, clientSecret);
                var records = await ReadPaginatedAsync(baseUrl, accessToken, resource, maxPages: 1);
                return records.Take(sampleSize);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Marketo: dry run preview failed for '{Resource}'.", resource);
                throw new ConnectorException($"Marketo dry run preview failed for '{resource}': {ex.Message}", ex, "marketo");
            }
        }

        public Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            return Task.FromResult<IEnumerable<string>>(AllResources);
        }

        // ── OAuth token acquisition ──────────────────────────────────────────

        private async Task<string> ObtainAccessTokenAsync(string baseUrl, string clientId, string clientSecret)
        {
            var url = $"{baseUrl}/identity/oauth/token?grant_type=client_credentials"
                + $"&client_id={Uri.EscapeDataString(clientId)}"
                + $"&client_secret={Uri.EscapeDataString(clientSecret)}";

            using var request = new HttpRequestMessage(HttpMethod.Post, url);
            using var response = await _httpClient.SendAsync(request);
            response.EnsureSuccessStatusCode();

            var json = await response.Content.ReadAsStringAsync();
            using var doc = JsonDocument.Parse(json);

            if (doc.RootElement.TryGetProperty("access_token", out var tokenEl)
                && tokenEl.ValueKind == JsonValueKind.String)
            {
                return tokenEl.GetString()!;
            }

            throw new ConnectorException(
                "Failed to obtain Marketo access token. Check clientId and clientSecret.",
                new InvalidOperationException("No access_token in response."),
                "marketo");
        }

        // ── Paginated read ───────────────────────────────────────────────────

        private async Task<List<object>> ReadPaginatedAsync(
            string baseUrl, string accessToken, string resource, int maxPages = int.MaxValue)
        {
            var results = new List<object>();
            string? nextPageToken = null;
            int page = 0;

            var endpoint = ResolveEndpoint(resource);

            do
            {
                var url = $"{baseUrl}/rest/v1/{endpoint}?batchSize={BatchSize}";
                if (!string.IsNullOrEmpty(nextPageToken))
                    url += $"&nextPageToken={Uri.EscapeDataString(nextPageToken)}";

                using var request = new HttpRequestMessage(HttpMethod.Get, url);
                ApplyAuth(request, accessToken);

                using var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);

                // Check for API-level errors.
                if (doc.RootElement.TryGetProperty("success", out var success)
                    && success.ValueKind == JsonValueKind.False)
                {
                    var errorMsg = "Unknown Marketo API error.";
                    if (doc.RootElement.TryGetProperty("errors", out var errors) && errors.ValueKind == JsonValueKind.Array)
                    {
                        var firstErr = errors.EnumerateArray().FirstOrDefault();
                        if (firstErr.TryGetProperty("message", out var msg))
                            errorMsg = msg.GetString() ?? errorMsg;
                    }
                    throw new ConnectorException(
                        $"Marketo API error for '{resource}': {errorMsg}",
                        new InvalidOperationException(errorMsg),
                        "marketo");
                }

                // Parse result array.
                if (doc.RootElement.TryGetProperty("result", out var resultArr) && resultArr.ValueKind == JsonValueKind.Array)
                {
                    ParseArray(resultArr, results);
                }

                // Pagination via nextPageToken.
                nextPageToken = null;
                if (doc.RootElement.TryGetProperty("moreResult", out var more)
                    && more.ValueKind == JsonValueKind.True
                    && doc.RootElement.TryGetProperty("nextPageToken", out var npt)
                    && npt.ValueKind == JsonValueKind.String)
                {
                    nextPageToken = npt.GetString();
                }

                page++;
            }
            while (nextPageToken != null && page < maxPages);

            _logger.LogInformation("Marketo: read {Count} records from '{Resource}' across {Pages} page(s).",
                results.Count, resource, page);

            return results;
        }

        // ── Lead schema describe ─────────────────────────────────────────────

        private async Task<IEnumerable<string>> DescribeLeadsAsync(string baseUrl, string accessToken)
        {
            var url = $"{baseUrl}/rest/v1/leads/describe.json";

            using var request = new HttpRequestMessage(HttpMethod.Get, url);
            ApplyAuth(request, accessToken);

            using var response = await _httpClient.SendAsync(request);
            response.EnsureSuccessStatusCode();

            var json = await response.Content.ReadAsStringAsync();
            using var doc = JsonDocument.Parse(json);

            var names = new List<string>();
            if (doc.RootElement.TryGetProperty("result", out var result) && result.ValueKind == JsonValueKind.Array)
            {
                foreach (var field in result.EnumerateArray())
                {
                    if (field.TryGetProperty("rest", out var rest)
                        && rest.TryGetProperty("name", out var name)
                        && name.ValueKind == JsonValueKind.String)
                    {
                        names.Add(name.GetString()!);
                    }
                }
            }

            return names;
        }

        // ── Endpoint mapping ─────────────────────────────────────────────────

        private static string ResolveEndpoint(string resource) => resource switch
        {
            "leads"           => "leads.json",
            "activities"      => "activities.json",
            "campaigns"       => "campaigns.json",
            "programs"        => "programs.json",
            "lists"           => "lists.json",
            "smart_lists"     => "smartLists.json",
            "smart_campaigns" => "smartCampaigns.json",
            "emails"          => "assets/emails.json",
            "landing_pages"   => "assets/landingPages.json",
            "forms"           => "assets/forms.json",
            "tokens"          => "assets/tokens.json",
            "channels"        => "channels.json",
            _                 => $"{resource}.json"
        };

        // ── Response parsing ─────────────────────────────────────────────────

        private static void ParseArray(JsonElement array, List<object> results)
        {
            foreach (var element in array.EnumerateArray())
            {
                IDictionary<string, object> row = new ExpandoObject();

                if (element.ValueKind == JsonValueKind.Object)
                {
                    foreach (var prop in element.EnumerateObject())
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

        // ── Credential resolution ────────────────────────────────────────────

        private (string munchkinId, string clientId, string clientSecret) ResolveCredentials(DataSourceConfig config)
        {
            var munchkinId = GetStringParam(config.Parameters, "munchkinId");
            var clientId = GetStringParam(config.Parameters, "clientId");
            var clientSecret = GetStringParam(config.Parameters, "clientSecret");

            // Fall back to ConnectionString JSON: {"munchkinId":"...","clientId":"...","clientSecret":"..."}
            if (string.IsNullOrEmpty(munchkinId) || string.IsNullOrEmpty(clientId) || string.IsNullOrEmpty(clientSecret))
            {
                if (!string.IsNullOrWhiteSpace(config.ConnectionString))
                {
                    try
                    {
                        using var doc = JsonDocument.Parse(config.ConnectionString);
                        if (string.IsNullOrEmpty(munchkinId) && doc.RootElement.TryGetProperty("munchkinId", out var mid))
                            munchkinId = mid.GetString();
                        if (string.IsNullOrEmpty(clientId) && doc.RootElement.TryGetProperty("clientId", out var cid))
                            clientId = cid.GetString();
                        if (string.IsNullOrEmpty(clientSecret) && doc.RootElement.TryGetProperty("clientSecret", out var cs))
                            clientSecret = cs.GetString();
                    }
                    catch (JsonException)
                    {
                        // ConnectionString is not valid JSON; ignore.
                    }
                }
            }

            if (string.IsNullOrEmpty(munchkinId))
                throw new ConnectorException(
                    "Marketo Munchkin ID is required. Provide it via Parameters['munchkinId'] or the connection string.",
                    new ArgumentException("Missing 'munchkinId'."),
                    "marketo");

            if (string.IsNullOrEmpty(clientId))
                throw new ConnectorException(
                    "Marketo client ID is required. Provide it via Parameters['clientId'] or the connection string.",
                    new ArgumentException("Missing 'clientId'."),
                    "marketo");

            if (string.IsNullOrEmpty(clientSecret))
                throw new ConnectorException(
                    "Marketo client secret is required. Provide it via Parameters['clientSecret'] or the connection string.",
                    new ArgumentException("Missing 'clientSecret'."),
                    "marketo");

            return (munchkinId, clientId, clientSecret);
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
                    $"Marketo connector requires the '{key}' parameter.",
                    new ArgumentException($"Missing required parameter: {key}"),
                    "marketo");
            return value;
        }
    }
}
