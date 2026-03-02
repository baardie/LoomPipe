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
    /// Reads form and response data from Typeform using its API.
    ///
    /// Parameters:
    ///   accessToken  — Typeform personal access token (Bearer)
    ///   resource     — forms, responses, workspaces, themes, images
    ///   formId       — required for responses
    /// </summary>
    public class TypeformReader : ISourceReader
    {
        private const string BaseUrl = "https://api.typeform.com";
        private const int PageLimit = 100;

        private static readonly string[] AllResources =
        {
            "forms", "responses", "workspaces", "themes", "images"
        };

        private readonly HttpClient _httpClient;
        private readonly ILogger<TypeformReader> _logger;

        public TypeformReader(HttpClient httpClient, ILogger<TypeformReader> logger)
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
                    "Typeform access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "typeform");
            var formId = GetStringParam(config.Parameters, "formId");

            _logger.LogInformation("Typeform: reading resource '{Resource}'.", resource);

            try
            {
                return await ReadFullAsync(resource, accessToken, formId);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Typeform: failed to read resource '{Resource}'.", resource);
                throw new ConnectorException($"Failed to read Typeform resource '{resource}': {ex.Message}", ex, "typeform");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Typeform access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "typeform");
            var formId = GetStringParam(config.Parameters, "formId");

            _logger.LogInformation("Typeform: discovering schema for '{Resource}'.", resource);

            try
            {
                var sample = await ReadFullAsync(resource, accessToken, formId, maxPages: 1);
                var first = sample.FirstOrDefault() as IDictionary<string, object>;
                return first?.Keys ?? Array.Empty<string>();
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Typeform: failed to discover schema for '{Resource}'.", resource);
                throw new ConnectorException($"Failed to discover Typeform schema for '{resource}': {ex.Message}", ex, "typeform");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Typeform access token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "typeform");
            var formId = GetStringParam(config.Parameters, "formId");

            _logger.LogInformation("Typeform: dry run preview for '{Resource}' (sample={SampleSize}).", resource, sampleSize);

            try
            {
                var records = await ReadFullAsync(resource, accessToken, formId, maxPages: 1);
                return records.Take(sampleSize);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Typeform: dry run preview failed for '{Resource}'.", resource);
                throw new ConnectorException($"Typeform dry run preview failed for '{resource}': {ex.Message}", ex, "typeform");
            }
        }

        public Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            return Task.FromResult<IEnumerable<string>>(AllResources);
        }

        // ── Full read (paginated GET) ────────────────────────────────────────

        private async Task<List<object>> ReadFullAsync(
            string resource, string accessToken, string? formId, int maxPages = int.MaxValue)
        {
            var results = new List<object>();

            if (resource == "responses")
            {
                if (string.IsNullOrWhiteSpace(formId))
                    throw new ConnectorException(
                        "Typeform connector requires the 'formId' parameter for the 'responses' resource.",
                        new ArgumentException("Missing 'formId'."),
                        "typeform");

                await ReadResponsesAsync(accessToken, formId, results, maxPages);
            }
            else if (resource == "forms")
            {
                await ReadFormsAsync(accessToken, results, maxPages);
            }
            else
            {
                await ReadGenericAsync(resource, accessToken, results, maxPages);
            }

            _logger.LogInformation("Typeform: read {Count} records from '{Resource}'.",
                results.Count, resource);

            return results;
        }

        /// <summary>
        /// Reads form responses with before-token pagination and flattens answers into columns.
        /// </summary>
        private async Task ReadResponsesAsync(
            string accessToken, string formId, List<object> results, int maxPages)
        {
            string? beforeToken = null;
            int page = 0;

            do
            {
                var sb = new StringBuilder($"{BaseUrl}/forms/{Uri.EscapeDataString(formId)}/responses");
                sb.Append($"?page_size={PageLimit}");
                if (!string.IsNullOrEmpty(beforeToken))
                {
                    sb.Append($"&before={Uri.EscapeDataString(beforeToken)}");
                }

                using var request = new HttpRequestMessage(HttpMethod.Get, sb.ToString());
                ApplyAuth(request, accessToken);

                using var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);

                int count = 0;

                if (doc.RootElement.TryGetProperty("items", out var items)
                    && items.ValueKind == JsonValueKind.Array)
                {
                    foreach (var element in items.EnumerateArray())
                    {
                        IDictionary<string, object> row = new ExpandoObject();

                        // Top-level response fields
                        if (element.TryGetProperty("response_id", out var rid))
                            row["response_id"] = rid.GetString() ?? rid.ToString();
                        if (element.TryGetProperty("landed_at", out var landed))
                            row["landed_at"] = landed.GetString() ?? landed.ToString();
                        if (element.TryGetProperty("submitted_at", out var submitted))
                            row["submitted_at"] = submitted.GetString() ?? submitted.ToString();
                        if (element.TryGetProperty("token", out var token))
                            row["token"] = token.GetString() ?? token.ToString();

                        // Flatten answers into columns by field ref/id
                        if (element.TryGetProperty("answers", out var answers)
                            && answers.ValueKind == JsonValueKind.Array)
                        {
                            foreach (var answer in answers.EnumerateArray())
                            {
                                var fieldKey = GetAnswerFieldKey(answer);
                                var fieldValue = GetAnswerValue(answer);
                                row[fieldKey] = fieldValue;
                            }
                        }

                        results.Add(row);
                        count++;
                    }
                }

                // Pagination: use the token of the last item as the "before" cursor
                beforeToken = null;
                if (count > 0 && count >= PageLimit)
                {
                    if (doc.RootElement.TryGetProperty("items", out var itemsArr)
                        && itemsArr.ValueKind == JsonValueKind.Array)
                    {
                        var lastItem = itemsArr.EnumerateArray().LastOrDefault();
                        if (lastItem.TryGetProperty("token", out var lastToken)
                            && lastToken.ValueKind == JsonValueKind.String)
                        {
                            beforeToken = lastToken.GetString();
                        }
                    }
                }

                page++;
            }
            while (beforeToken != null && page < maxPages);
        }

        /// <summary>
        /// Reads forms with page-based pagination.
        /// </summary>
        private async Task ReadFormsAsync(string accessToken, List<object> results, int maxPages)
        {
            int pageNumber = 1;
            int pageCount = 0;

            do
            {
                var url = $"{BaseUrl}/forms?page_size=200&page={pageNumber}";

                using var request = new HttpRequestMessage(HttpMethod.Get, url);
                ApplyAuth(request, accessToken);

                using var response = await _httpClient.SendAsync(request);
                response.EnsureSuccessStatusCode();

                var json = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);

                if (doc.RootElement.TryGetProperty("items", out var items)
                    && items.ValueKind == JsonValueKind.Array)
                {
                    foreach (var element in items.EnumerateArray())
                    {
                        IDictionary<string, object> row = new ExpandoObject();
                        foreach (var prop in element.EnumerateObject())
                        {
                            row[prop.Name] = ConvertJsonValue(prop.Value);
                        }
                        results.Add(row);
                    }
                }

                if (doc.RootElement.TryGetProperty("page_count", out var pc)
                    && pc.ValueKind == JsonValueKind.Number)
                {
                    pageCount = pc.GetInt32();
                }

                pageNumber++;
            }
            while (pageNumber <= pageCount && (pageNumber - 1) < maxPages);
        }

        /// <summary>
        /// Reads generic resources (workspaces, themes, images) with simple item parsing.
        /// </summary>
        private async Task ReadGenericAsync(
            string resource, string accessToken, List<object> results, int maxPages)
        {
            var url = $"{BaseUrl}/{resource}";

            using var request = new HttpRequestMessage(HttpMethod.Get, url);
            ApplyAuth(request, accessToken);

            using var response = await _httpClient.SendAsync(request);
            response.EnsureSuccessStatusCode();

            var json = await response.Content.ReadAsStringAsync();
            using var doc = JsonDocument.Parse(json);

            // Try "items" first, then the root as array
            JsonElement items;
            if (doc.RootElement.TryGetProperty("items", out items) && items.ValueKind == JsonValueKind.Array)
            {
                // standard response
            }
            else if (doc.RootElement.ValueKind == JsonValueKind.Array)
            {
                items = doc.RootElement;
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
                    row[prop.Name] = ConvertJsonValue(prop.Value);
                }
                results.Add(row);
            }
        }

        // ── Answer flattening helpers ────────────────────────────────────────

        private static string GetAnswerFieldKey(JsonElement answer)
        {
            // Use field.ref if available, otherwise field.id
            if (answer.TryGetProperty("field", out var field))
            {
                if (field.TryGetProperty("ref", out var refEl) && refEl.ValueKind == JsonValueKind.String)
                    return refEl.GetString() ?? "unknown_field";
                if (field.TryGetProperty("id", out var idEl) && idEl.ValueKind == JsonValueKind.String)
                    return idEl.GetString() ?? "unknown_field";
            }
            return "unknown_field";
        }

        private static object GetAnswerValue(JsonElement answer)
        {
            if (!answer.TryGetProperty("type", out var typeEl) || typeEl.ValueKind != JsonValueKind.String)
                return string.Empty;

            var type = typeEl.GetString() ?? "";

            // Try to get the value from the type-named property
            if (answer.TryGetProperty(type, out var val))
            {
                return val.ValueKind switch
                {
                    JsonValueKind.String => (object)(val.GetString() ?? string.Empty),
                    JsonValueKind.Number => val.TryGetInt64(out var l) ? l : val.GetDouble(),
                    JsonValueKind.True   => true,
                    JsonValueKind.False  => false,
                    JsonValueKind.Null   => string.Empty,
                    // For objects (e.g. choice) or arrays (e.g. choices), serialise to string
                    _                    => val.ToString()
                };
            }

            return string.Empty;
        }

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
                    $"Typeform connector requires the '{key}' parameter.",
                    new ArgumentException($"Missing required parameter: {key}"),
                    "typeform");
            return value;
        }
    }
}
