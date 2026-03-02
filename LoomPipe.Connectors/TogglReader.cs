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
    /// Reads data from Toggl Track using the v9 API.
    ///
    /// Parameters:
    ///   accessToken  — Toggl API token (used as Basic auth username with "api_token" as password)
    ///   workspaceId  — Toggl workspace ID (required for workspace-scoped resources)
    ///   resource     — time_entries, projects, clients, tasks, tags, workspaces,
    ///                  organizations, groups
    /// </summary>
    public class TogglReader : ISourceReader
    {
        private const string BaseUrl = "https://api.track.toggl.com/api/v9";

        private static readonly string[] AllResources =
        {
            "time_entries", "projects", "clients", "tasks", "tags",
            "workspaces", "organizations", "groups"
        };

        /// <summary>
        /// Resources that are scoped to a workspace and require workspaceId.
        /// </summary>
        private static readonly HashSet<string> WorkspaceResources = new(StringComparer.OrdinalIgnoreCase)
        {
            "projects", "clients", "tasks", "tags", "groups"
        };

        private readonly HttpClient _httpClient;
        private readonly ILogger<TogglReader> _logger;

        public TogglReader(HttpClient httpClient, ILogger<TogglReader> logger)
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
                    "Toggl API token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "toggl");
            var workspaceId = GetStringParam(config.Parameters, "workspaceId");

            if (WorkspaceResources.Contains(resource) && string.IsNullOrWhiteSpace(workspaceId))
            {
                throw new ConnectorException(
                    $"Toggl resource '{resource}' requires the 'workspaceId' parameter.",
                    new ArgumentException("Missing 'workspaceId'."),
                    "toggl");
            }

            _logger.LogInformation("Toggl: reading resource '{Resource}'.", resource);

            try
            {
                return await ReadFullAsync(resource, accessToken, workspaceId);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Toggl: failed to read resource '{Resource}'.", resource);
                throw new ConnectorException($"Failed to read Toggl resource '{resource}': {ex.Message}", ex, "toggl");
            }
        }

        public async Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Toggl API token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "toggl");
            var workspaceId = GetStringParam(config.Parameters, "workspaceId");

            _logger.LogInformation("Toggl: discovering schema for '{Resource}'.", resource);

            try
            {
                var sample = await ReadFullAsync(resource, accessToken, workspaceId);
                var first = sample.FirstOrDefault() as IDictionary<string, object>;
                return first?.Keys ?? Array.Empty<string>();
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Toggl: failed to discover schema for '{Resource}'.", resource);
                throw new ConnectorException($"Failed to discover Toggl schema for '{resource}': {ex.Message}", ex, "toggl");
            }
        }

        public async Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10)
        {
            var resource    = GetRequiredParam(config.Parameters, "resource");
            var accessToken = GetStringParam(config.Parameters, "accessToken")
                ?? (string.IsNullOrWhiteSpace(config.ConnectionString) ? null : config.ConnectionString)
                ?? throw new ConnectorException(
                    "Toggl API token is required. Provide it via Parameters['accessToken'] or the connection string.",
                    new ArgumentException("Missing 'accessToken'."),
                    "toggl");
            var workspaceId = GetStringParam(config.Parameters, "workspaceId");

            _logger.LogInformation("Toggl: dry run preview for '{Resource}' (sample={SampleSize}).", resource, sampleSize);

            try
            {
                var records = await ReadFullAsync(resource, accessToken, workspaceId);
                return records.Take(sampleSize);
            }
            catch (ConnectorException) { throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Toggl: dry run preview failed for '{Resource}'.", resource);
                throw new ConnectorException($"Toggl dry run preview failed for '{resource}': {ex.Message}", ex, "toggl");
            }
        }

        public Task<IEnumerable<string>> ListResourcesAsync(DataSourceConfig config)
        {
            return Task.FromResult<IEnumerable<string>>(AllResources);
        }

        // ── Full read (single GET — most Toggl endpoints return all data) ────

        private async Task<List<object>> ReadFullAsync(
            string resource, string accessToken, string? workspaceId)
        {
            var results = new List<object>();
            var url = BuildUrl(resource, workspaceId);

            using var request = new HttpRequestMessage(HttpMethod.Get, url);
            ApplyAuth(request, accessToken);

            using var response = await _httpClient.SendAsync(request);
            response.EnsureSuccessStatusCode();

            var json = await response.Content.ReadAsStringAsync();
            using var doc = JsonDocument.Parse(json);

            var root = doc.RootElement;

            if (root.ValueKind == JsonValueKind.Array)
            {
                ParseArray(root, results);
            }
            else if (root.TryGetProperty("data", out var dataArray) && dataArray.ValueKind == JsonValueKind.Array)
            {
                ParseArray(dataArray, results);
            }

            _logger.LogInformation("Toggl: read {Count} records from '{Resource}'.",
                results.Count, resource);

            return results;
        }

        // ── URL builder ─────────────────────────────────────────────────────

        private static string BuildUrl(string resource, string? workspaceId) => resource switch
        {
            "time_entries"  => $"{BaseUrl}/me/time_entries",
            "workspaces"    => $"{BaseUrl}/me/workspaces",
            "organizations" => $"{BaseUrl}/me/organizations",
            "projects"      => $"{BaseUrl}/workspaces/{workspaceId}/projects",
            "clients"       => $"{BaseUrl}/workspaces/{workspaceId}/clients",
            "tasks"         => $"{BaseUrl}/workspaces/{workspaceId}/tasks",
            "tags"          => $"{BaseUrl}/workspaces/{workspaceId}/tags",
            "groups"        => $"{BaseUrl}/workspaces/{workspaceId}/groups",
            _               => $"{BaseUrl}/me/{resource}"
        };

        // ── Response parsing ────────────────────────────────────────────────

        private static void ParseArray(JsonElement items, List<object> results)
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

        // ── Auth helper ─────────────────────────────────────────────────────

        private static void ApplyAuth(HttpRequestMessage request, string accessToken)
        {
            // Toggl uses Basic auth with the API token as username and "api_token" as password.
            var credentials = Convert.ToBase64String(Encoding.ASCII.GetBytes($"{accessToken}:api_token"));
            request.Headers.Authorization = new AuthenticationHeaderValue("Basic", credentials);
        }

        // ── Parameter helpers ───────────────────────────────────────────────

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
                    $"Toggl connector requires the '{key}' parameter.",
                    new ArgumentException($"Missing required parameter: {key}"),
                    "toggl");
            return value;
        }
    }
}
