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
    /// Reads data from the Twilio REST API (2010-04-01).
    ///
    /// Parameters:
    ///   accountSid   — Twilio Account SID
    ///   authToken    — Twilio Auth Token
    ///   resource     — messages, calls, accounts, recordings, transcriptions,
    ///                  phone_numbers, queues, conferences, usage
    ///
    /// Alternatively, provide a JSON connection string: {"accountSid":"...","authToken":"..."}
    /// </summary>
    public class TwilioReader : ISourceReader
    {
        private const string BaseUrl = "https://api.twilio.com/2010-04-01";
        private const int PageSize = 100;

        private static readonly string[] AllResources =
        {
            "messages", "calls", "accounts", "recordings", "transcriptions",
            "phone_numbers", "queues", "conferences", "usage"
        };

        private readonly HttpClient _httpClient;
        private readonly ILogger<TwilioReader> _logger;

        public TwilioReader(HttpClient httpClient, ILogger<TwilioReader> logger)
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
            var (accountSid, authToken) = ResolveCredentials(config);

            _logger.LogInformation("Twilio: reading resource '{Resource}'.", resource);

            try
            {
                return await ReadFullAsync(resource, accountSid, authToken);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Twilio: failed to read resource '{Resource}'.", resource);
                throw new ConnectorException($"Failed to read Twilio resource '{resource}': {ex.Message}", ex, "twilio");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var resource = GetRequiredParam(config.Parameters, "resource");
            var (accountSid, authToken) = ResolveCredentials(config);

            _logger.LogInformation("Twilio: discovering schema for '{Resource}'.", resource);

            try
            {
                var sample = await ReadFullAsync(resource, accountSid, authToken, maxPages: 1);
                var first = sample.FirstOrDefault() as IDictionary<string, object>;
                return first?.Keys ?? Array.Empty<string>();
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Twilio: failed to discover schema for '{Resource}'.", resource);
                throw new ConnectorException($"Failed to discover Twilio schema for '{resource}': {ex.Message}", ex, "twilio");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var resource = GetRequiredParam(config.Parameters, "resource");
            var (accountSid, authToken) = ResolveCredentials(config);

            _logger.LogInformation("Twilio: dry run preview for '{Resource}' (sample={SampleSize}).", resource, sampleSize);

            try
            {
                var records = await ReadFullAsync(resource, accountSid, authToken, maxPages: 1);
                return records.Take(sampleSize);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Twilio: dry run preview failed for '{Resource}'.", resource);
                throw new ConnectorException($"Twilio dry run preview failed for '{resource}': {ex.Message}", ex, "twilio");
            }
        }

        public Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            return Task.FromResult<IEnumerable<string>>(AllResources);
        }

        // ── Full read (paginated GET) ────────────────────────────────────────

        private async Task<List<object>> ReadFullAsync(
            string resource, string accountSid, string authToken, int maxPages = int.MaxValue)
        {
            var results = new List<object>();
            string? nextPageUri = null;
            int page = 0;

            // Build the initial URL.
            var url = BuildUrl(resource, accountSid);

            do
            {
                var requestUrl = nextPageUri != null
                    ? $"https://api.twilio.com{nextPageUri}"
                    : url;

                using var request = new HttpRequestMessage(HttpMethod.Get, requestUrl);
                ApplyAuth(request, accountSid, authToken);

                using var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);

                var resourceKey = GetResourceKey(resource);
                ParseResultsPage(doc.RootElement, resourceKey, results);

                // Pagination: Twilio uses "next_page_uri"
                nextPageUri = null;
                if (doc.RootElement.TryGetProperty("next_page_uri", out var nextUri)
                    && nextUri.ValueKind == JsonValueKind.String)
                {
                    nextPageUri = nextUri.GetString();
                }

                page++;
            }
            while (nextPageUri != null && page < maxPages);

            _logger.LogInformation("Twilio: read {Count} records from '{Resource}' across {Pages} page(s).",
                results.Count, resource, page);

            return results;
        }

        // ── URL builder ──────────────────────────────────────────────────────

        private static string BuildUrl(string resource, string accountSid)
        {
            var endpoint = resource switch
            {
                "messages"        => $"/Accounts/{accountSid}/Messages.json",
                "calls"           => $"/Accounts/{accountSid}/Calls.json",
                "accounts"        => "/Accounts.json",
                "recordings"      => $"/Accounts/{accountSid}/Recordings.json",
                "transcriptions"  => $"/Accounts/{accountSid}/Transcriptions.json",
                "phone_numbers"   => $"/Accounts/{accountSid}/IncomingPhoneNumbers.json",
                "queues"          => $"/Accounts/{accountSid}/Queues.json",
                "conferences"     => $"/Accounts/{accountSid}/Conferences.json",
                "usage"           => $"/Accounts/{accountSid}/Usage/Records.json",
                _ => throw new ConnectorException(
                    $"Twilio: unsupported resource '{resource}'.",
                    new ArgumentException($"Unsupported resource: {resource}"),
                    "twilio")
            };

            return $"{BaseUrl}{endpoint}?PageSize={PageSize}";
        }

        /// <summary>
        /// Returns the JSON property name that wraps the results array for a given resource.
        /// </summary>
        private static string GetResourceKey(string resource) => resource switch
        {
            "messages"        => "messages",
            "calls"           => "calls",
            "accounts"        => "accounts",
            "recordings"      => "recordings",
            "transcriptions"  => "transcriptions",
            "phone_numbers"   => "incoming_phone_numbers",
            "queues"          => "queues",
            "conferences"     => "conferences",
            "usage"           => "usage_records",
            _                 => resource
        };

        // ── Response parsing ─────────────────────────────────────────────────

        private static void ParseResultsPage(JsonElement root, string resourceKey, List<object> results)
        {
            JsonElement items;

            if (root.TryGetProperty(resourceKey, out items) && items.ValueKind == JsonValueKind.Array)
            {
                // Standard Twilio response shape
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
                        // Flatten nested objects like "subresource_uris"
                        foreach (var nested in prop.Value.EnumerateObject())
                        {
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

        private static void ApplyAuth(HttpRequestMessage request, string accountSid, string authToken)
        {
            var credentials = Convert.ToBase64String(Encoding.ASCII.GetBytes($"{accountSid}:{authToken}"));
            request.Headers.Authorization = new AuthenticationHeaderValue("Basic", credentials);
        }

        // ── Credential resolution ────────────────────────────────────────────

        private (string AccountSid, string AuthToken) ResolveCredentials(DataSourceConfig config)
        {
            var accountSid = GetStringParam(config.Parameters, "accountSid");
            var authToken  = GetStringParam(config.Parameters, "authToken");

            if (!string.IsNullOrWhiteSpace(accountSid) && !string.IsNullOrWhiteSpace(authToken))
                return (accountSid, authToken);

            // Fall back to connection string JSON: {"accountSid":"...","authToken":"..."}
            if (!string.IsNullOrWhiteSpace(config.ConnectionString))
            {
                try
                {
                    using var doc = JsonDocument.Parse(config.ConnectionString);
                    var root = doc.RootElement;

                    var sid   = root.TryGetProperty("accountSid", out var s) ? s.GetString() : null;
                    var token = root.TryGetProperty("authToken", out var t) ? t.GetString() : null;

                    if (!string.IsNullOrWhiteSpace(sid) && !string.IsNullOrWhiteSpace(token))
                        return (sid!, token!);
                }
                catch (JsonException)
                {
                    // Not valid JSON — fall through to error.
                }
            }

            throw new ConnectorException(
                "Twilio credentials are required. Provide 'accountSid' and 'authToken' via Parameters or a JSON connection string.",
                new ArgumentException("Missing Twilio credentials."),
                "twilio");
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
                    $"Twilio connector requires the '{key}' parameter.",
                    new ArgumentException($"Missing required parameter: {key}"),
                    "twilio");
            return value;
        }
    }
}
