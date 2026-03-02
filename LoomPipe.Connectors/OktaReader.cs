#nullable enable
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text.Json;
using System.Threading.Tasks;
using LoomPipe.Core.Entities;
using LoomPipe.Core.Exceptions;
using LoomPipe.Core.Interfaces;
using Microsoft.Extensions.Logging;

namespace LoomPipe.Connectors
{
    /// <summary>
    /// Reads data from Okta using the v1 API.
    ///
    /// Parameters:
    ///   accessToken  — Okta API token (used with SSWS auth scheme)
    ///   domain       — Okta domain (e.g. "mycompany" → mycompany.okta.com)
    ///   resource     — users, groups, apps, logs, factors, policies, roles, sessions,
    ///                  identity_providers, authorization_servers
    ///
    /// ConnectionString JSON: {"domain":"...","accessToken":"..."}
    /// </summary>
    public class OktaReader : ISourceReader
    {
        private const int PageLimitDefault = 200;
        private const int PageLimitLogs = 1000;

        private static readonly string[] AllResources =
        {
            "users", "groups", "apps", "logs", "factors", "policies",
            "roles", "sessions", "identity_providers", "authorization_servers"
        };

        private readonly HttpClient _httpClient;
        private readonly ILogger<OktaReader> _logger;

        public OktaReader(HttpClient httpClient, ILogger<OktaReader> logger)
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
            var (domain, accessToken) = ResolveAuth(config);
            var resource = GetRequiredParam(config.Parameters, "resource");

            _logger.LogInformation("Okta: reading resource '{Resource}' from '{Domain}'.", resource, domain);

            try
            {
                return await ReadFullAsync(domain, accessToken, resource);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Okta: failed to read resource '{Resource}'.", resource);
                throw new ConnectorException($"Failed to read Okta resource '{resource}': {ex.Message}", ex, "okta");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var (domain, accessToken) = ResolveAuth(config);
            var resource = GetRequiredParam(config.Parameters, "resource");

            _logger.LogInformation("Okta: discovering schema for '{Resource}'.", resource);

            try
            {
                var sample = await ReadFullAsync(domain, accessToken, resource, maxPages: 1);
                var first = sample.FirstOrDefault() as IDictionary<string, object>;
                return first?.Keys ?? Array.Empty<string>();
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Okta: failed to discover schema for '{Resource}'.", resource);
                throw new ConnectorException($"Failed to discover Okta schema for '{resource}': {ex.Message}", ex, "okta");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var (domain, accessToken) = ResolveAuth(config);
            var resource = GetRequiredParam(config.Parameters, "resource");

            _logger.LogInformation("Okta: dry run preview for '{Resource}' (sample={SampleSize}).", resource, sampleSize);

            try
            {
                var records = await ReadFullAsync(domain, accessToken, resource, maxPages: 1);
                return records.Take(sampleSize);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Okta: dry run preview failed for '{Resource}'.", resource);
                throw new ConnectorException($"Okta dry run preview failed for '{resource}': {ex.Message}", ex, "okta");
            }
        }

        public Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            return Task.FromResult<IEnumerable<string>>(AllResources);
        }

        // ── Full read (Link header pagination) ──────────────────────────────

        private async Task<List<object>> ReadFullAsync(
            string domain, string accessToken, string resource, int maxPages = int.MaxValue)
        {
            var baseUrl = $"https://{domain}.okta.com";
            var results = new List<object>();
            var url = BuildUrl(baseUrl, resource);
            int page = 0;

            do
            {
                using var request = new HttpRequestMessage(HttpMethod.Get, url);
                ApplyAuth(request, accessToken);

                using var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);
                var root = doc.RootElement;

                // Okta returns arrays directly for most endpoints
                if (root.ValueKind == JsonValueKind.Array)
                {
                    foreach (var el in root.EnumerateArray())
                        results.Add(FlattenJsonObject(el));
                }
                else if (root.ValueKind == JsonValueKind.Object)
                {
                    // Some endpoints (e.g. logs) may wrap in an object — flatten top-level
                    results.Add(FlattenJsonObject(root));
                }

                // Pagination via Link header with rel="next"
                url = ParseNextLinkFromHeaders(response);
                page++;
            }
            while (url != null && page < maxPages);

            _logger.LogInformation("Okta: read {Count} records from '{Resource}' across {Pages} page(s).",
                results.Count, resource, page);

            return results;
        }

        // ── URL builders ─────────────────────────────────────────────────────

        private static string BuildUrl(string baseUrl, string resource)
        {
            return resource switch
            {
                "users"                 => $"{baseUrl}/api/v1/users?limit={PageLimitDefault}",
                "groups"                => $"{baseUrl}/api/v1/groups?limit={PageLimitDefault}",
                "apps"                  => $"{baseUrl}/api/v1/apps?limit={PageLimitDefault}",
                "logs"                  => $"{baseUrl}/api/v1/logs?limit={PageLimitLogs}",
                "factors"               => $"{baseUrl}/api/v1/users?limit={PageLimitDefault}",  // factors are per-user; read users first
                "policies"              => $"{baseUrl}/api/v1/policies?limit={PageLimitDefault}",
                "roles"                 => $"{baseUrl}/api/v1/iam/roles?limit={PageLimitDefault}",
                "sessions"              => $"{baseUrl}/api/v1/sessions?limit={PageLimitDefault}",
                "identity_providers"    => $"{baseUrl}/api/v1/idps?limit={PageLimitDefault}",
                "authorization_servers" => $"{baseUrl}/api/v1/authorizationServers?limit={PageLimitDefault}",
                _ => throw new ConnectorException(
                    $"Unknown Okta resource '{resource}'.",
                    new ArgumentException($"Unsupported resource: {resource}"),
                    "okta")
            };
        }

        // ── Link header pagination ──────────────────────────────────────────

        /// <summary>
        /// Parses the Link header to extract the next page URL.
        /// Okta uses: Link: &lt;url&gt;; rel="next"
        /// </summary>
        private static string? ParseNextLinkFromHeaders(HttpResponseMessage response)
        {
            if (!response.Headers.TryGetValues("Link", out var linkValues))
                return null;

            foreach (var linkHeader in linkValues)
            {
                var parts = linkHeader.Split(',');
                foreach (var part in parts)
                {
                    var trimmed = part.Trim();
                    if (trimmed.Contains("rel=\"next\""))
                    {
                        // Extract URL between < and >
                        var urlStart = trimmed.IndexOf('<');
                        var urlEnd = trimmed.IndexOf('>');
                        if (urlStart >= 0 && urlEnd > urlStart)
                        {
                            return trimmed.Substring(urlStart + 1, urlEnd - urlStart - 1);
                        }
                    }
                }
            }

            return null;
        }

        // ── JSON flattening ─────────────────────────────────────────────────

        private static object FlattenJsonObject(JsonElement element)
        {
            IDictionary<string, object> row = new ExpandoObject();

            if (element.ValueKind == JsonValueKind.Object)
            {
                foreach (var prop in element.EnumerateObject())
                {
                    row[prop.Name] = ConvertJsonValue(prop.Value);
                }
            }

            return row;
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

        // ── Auth helpers ─────────────────────────────────────────────────────

        private (string domain, string accessToken) ResolveAuth(DataSourceConfig config)
        {
            MergeConnectionString(config);

            var domain = GetRequiredParam(config.Parameters, "domain");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Okta API token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "okta");

            return (domain, accessToken);
        }

        /// <summary>
        /// If ConnectionString is a JSON object with domain/accessToken, merge those
        /// into Parameters so the user can configure everything from the connection string.
        /// </summary>
        private static void MergeConnectionString(DataSourceConfig config)
        {
            if (string.IsNullOrWhiteSpace(config.ConnectionString)) return;

            try
            {
                using var doc = JsonDocument.Parse(config.ConnectionString);
                var root = doc.RootElement;
                if (root.ValueKind != JsonValueKind.Object) return;

                foreach (var prop in root.EnumerateObject())
                {
                    // Only merge if the parameter isn't already explicitly set.
                    if (!config.Parameters.ContainsKey(prop.Name) && prop.Value.ValueKind == JsonValueKind.String)
                    {
                        config.Parameters[prop.Name] = prop.Value.GetString()!;
                    }
                }
            }
            catch (JsonException)
            {
                // Not JSON — treat as a plain access token.
            }
        }

        /// <summary>
        /// Applies Okta SSWS authorization header.
        /// </summary>
        private static void ApplyAuth(HttpRequestMessage request, string accessToken)
        {
            request.Headers.Authorization = new AuthenticationHeaderValue("SSWS", accessToken);
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
                    $"Okta connector requires the '{key}' parameter.",
                    new ArgumentException($"Missing required parameter: {key}"),
                    "okta");
            return value;
        }
    }
}
