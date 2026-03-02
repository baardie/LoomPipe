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
    /// Reads survey data from SurveyMonkey using the v3 API.
    ///
    /// Parameters:
    ///   accessToken  — SurveyMonkey OAuth bearer token
    ///   resource     — surveys, responses, collectors, contacts, groups,
    ///                  workgroups, users
    ///   surveyId     — required for responses and collectors
    /// </summary>
    public class SurveyMonkeyReader : ISourceReader
    {
        private const string BaseUrl = "https://api.surveymonkey.com/v3";
        private const int PageLimit = 100;

        private static readonly string[] AllResources =
        {
            "surveys", "responses", "collectors", "contacts",
            "groups", "workgroups", "users"
        };

        private readonly HttpClient _httpClient;
        private readonly ILogger<SurveyMonkeyReader> _logger;

        public SurveyMonkeyReader(HttpClient httpClient, ILogger<SurveyMonkeyReader> logger)
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
                    "SurveyMonkey access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "surveymonkey");
            var surveyId = GetStringParam(config.Parameters, "surveyId");

            _logger.LogInformation("SurveyMonkey: reading resource '{Resource}'.", resource);

            try
            {
                return await ReadFullAsync(resource, accessToken, surveyId);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "SurveyMonkey: failed to read resource '{Resource}'.", resource);
                throw new ConnectorException($"Failed to read SurveyMonkey resource '{resource}': {ex.Message}", ex, "surveymonkey");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "SurveyMonkey access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "surveymonkey");
            var surveyId = GetStringParam(config.Parameters, "surveyId");

            _logger.LogInformation("SurveyMonkey: discovering schema for '{Resource}'.", resource);

            try
            {
                var sample = await ReadFullAsync(resource, accessToken, surveyId, maxPages: 1);
                var first = sample.FirstOrDefault() as IDictionary<string, object>;
                return first?.Keys ?? Array.Empty<string>();
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "SurveyMonkey: failed to discover schema for '{Resource}'.", resource);
                throw new ConnectorException($"Failed to discover SurveyMonkey schema for '{resource}': {ex.Message}", ex, "surveymonkey");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "SurveyMonkey access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "surveymonkey");
            var surveyId = GetStringParam(config.Parameters, "surveyId");

            _logger.LogInformation("SurveyMonkey: dry run preview for '{Resource}' (sample={SampleSize}).", resource, sampleSize);

            try
            {
                var records = await ReadFullAsync(resource, accessToken, surveyId, maxPages: 1);
                return records.Take(sampleSize);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "SurveyMonkey: dry run preview failed for '{Resource}'.", resource);
                throw new ConnectorException($"SurveyMonkey dry run preview failed for '{resource}': {ex.Message}", ex, "surveymonkey");
            }
        }

        public Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            return Task.FromResult<IEnumerable<string>>(AllResources);
        }

        // ── Full read (paginated GET) ────────────────────────────────────────

        private async Task<List<object>> ReadFullAsync(
            string resource, string accessToken, string? surveyId, int maxPages = int.MaxValue)
        {
            var results = new List<object>();
            int pageNumber = 1;
            int page = 0;

            do
            {
                var url = BuildListUrl(resource, surveyId, pageNumber);

                using var request = new HttpRequestMessage(HttpMethod.Get, url);
                ApplyAuth(request, accessToken);

                using var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);

                var dataKey = GetDataKey(resource);
                int count = 0;

                if (doc.RootElement.TryGetProperty(dataKey, out var data)
                    && data.ValueKind == JsonValueKind.Array)
                {
                    foreach (var element in data.EnumerateArray())
                    {
                        IDictionary<string, object> row = new ExpandoObject();
                        foreach (var prop in element.EnumerateObject())
                        {
                            row[prop.Name] = ConvertJsonValue(prop.Value);
                        }
                        results.Add(row);
                        count++;
                    }
                }

                // Pagination: check if there are more pages via links.next
                bool hasMore = false;
                if (doc.RootElement.TryGetProperty("links", out var links)
                    && links.TryGetProperty("next", out var nextLink)
                    && nextLink.ValueKind == JsonValueKind.String
                    && !string.IsNullOrEmpty(nextLink.GetString()))
                {
                    hasMore = true;
                }

                if (!hasMore || count == 0)
                    break;

                pageNumber++;
                page++;
            }
            while (page < maxPages);

            _logger.LogInformation("SurveyMonkey: read {Count} records from '{Resource}' across {Pages} page(s).",
                results.Count, resource, page + 1);

            return results;
        }

        // ── URL builder ─────────────────────────────────────────────────────

        private static string BuildListUrl(string resource, string? surveyId, int pageNumber)
        {
            string baseEndpoint = resource switch
            {
                "surveys"     => $"{BaseUrl}/surveys",
                "responses"   => $"{BaseUrl}/surveys/{surveyId}/responses/bulk",
                "collectors"  => $"{BaseUrl}/surveys/{surveyId}/collectors",
                "contacts"    => $"{BaseUrl}/contacts",
                "groups"      => $"{BaseUrl}/groups",
                "workgroups"  => $"{BaseUrl}/workgroups",
                "users"       => $"{BaseUrl}/users",
                _             => $"{BaseUrl}/{resource}"
            };

            var sb = new StringBuilder(baseEndpoint);
            sb.Append($"?per_page={PageLimit}");
            sb.Append($"&page={pageNumber}");

            return sb.ToString();
        }

        /// <summary>
        /// Returns the JSON property name containing the array of items for a given resource.
        /// </summary>
        private static string GetDataKey(string resource) => resource switch
        {
            "surveys"    => "data",
            "responses"  => "data",
            "collectors" => "data",
            "contacts"   => "data",
            "groups"     => "data",
            "workgroups" => "data",
            "users"      => "data",
            _            => "data"
        };

        // ── Response parsing ─────────────────────────────────────────────────

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
                    $"SurveyMonkey connector requires the '{key}' parameter.",
                    new ArgumentException($"Missing required parameter: {key}"),
                    "surveymonkey");
            return value;
        }
    }
}
