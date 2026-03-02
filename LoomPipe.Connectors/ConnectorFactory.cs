#nullable enable
using System;
using System.Data.Common;
using System.Diagnostics;
using System.Text.Json;
using System.Threading.Tasks;
using LoomPipe.Core.DTOs;
using LoomPipe.Core.Interfaces;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using MySqlConnector;
using Neo4j.Driver;
using Npgsql;
using Oracle.ManagedDataAccess.Client;
using Amazon.S3;
using Amazon.S3.Model;
using Pinecone;
using Snowflake.Data.Client;

namespace LoomPipe.Connectors
{
    public class ConnectorFactory : IConnectorFactory
    {
        private readonly IHttpClientFactory _httpClientFactory;
        private readonly ILoggerFactory _loggerFactory;

        public ConnectorFactory(IHttpClientFactory httpClientFactory, ILoggerFactory loggerFactory)
        {
            _httpClientFactory = httpClientFactory;
            _loggerFactory = loggerFactory;
        }

        // ── Source Readers ────────────────────────────────────────────────────

        public ISourceReader CreateSourceReader(string type) => type.ToLowerInvariant() switch
        {
            "csv"        => new CsvSourceReader(_loggerFactory.CreateLogger<CsvSourceReader>()),
            "json"       => new JsonSourceReader(_loggerFactory.CreateLogger<JsonSourceReader>()),
            "rest"       => new RestSourceReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<RestSourceReader>()),
            "sqlserver"  => new RelationalDbReader("sqlserver",  _loggerFactory.CreateLogger<RelationalDbReader>()),
            "postgresql" => new RelationalDbReader("postgresql", _loggerFactory.CreateLogger<RelationalDbReader>()),
            "mysql"      => new RelationalDbReader("mysql",      _loggerFactory.CreateLogger<RelationalDbReader>()),
            "oracle"     => new RelationalDbReader("oracle",     _loggerFactory.CreateLogger<RelationalDbReader>()),
            "mongodb"    => new MongoDbReader(_loggerFactory.CreateLogger<MongoDbReader>()),
            "neo4j"      => new Neo4jReader(_loggerFactory.CreateLogger<Neo4jReader>()),
            "snowflake"  => new SnowflakeReader(_loggerFactory.CreateLogger<SnowflakeReader>()),
            "bigquery"   => new BigQueryReader(_loggerFactory.CreateLogger<BigQueryReader>()),
            "pinecone"   => new PineconeReader(_loggerFactory.CreateLogger<PineconeReader>()),
            "milvus"     => new MilvusReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<MilvusReader>()),
            "stripe"     => new StripeReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<StripeReader>()),
            "shopify"      => new ShopifyReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<ShopifyReader>()),
            "googlesheets" => new GoogleSheetsReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<GoogleSheetsReader>()),
            "s3"           => new S3Reader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<S3Reader>()),
            "hubspot"      => new HubSpotReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<HubSpotReader>()),
            "salesforce"   => new SalesforceReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<SalesforceReader>()),
            "zendesk"      => new ZendeskReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<ZendeskReader>()),
            "pipedrive"    => new PipedriveReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<PipedriveReader>()),
            "jira"         => new JiraReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<JiraReader>()),
            "github"       => new GitHubReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<GitHubReader>()),
            "airtable"         => new AirtableReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<AirtableReader>()),
            "googleads"        => new GoogleAdsReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<GoogleAdsReader>()),
            "facebookads"      => new FacebookAdsReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<FacebookAdsReader>()),
            "linkedinads"      => new LinkedInAdsReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<LinkedInAdsReader>()),
            "googleanalytics"  => new GoogleAnalyticsReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<GoogleAnalyticsReader>()),
            "cassandra"        => new CassandraReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<CassandraReader>()),
            "clickhouse"       => new ClickHouseReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<ClickHouseReader>()),
            "databricks"       => new DatabricksReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<DatabricksReader>()),
            "redshift"         => new RedshiftReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<RedshiftReader>()),
            "sfcc"             => new SfccReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<SfccReader>()),
            "sap"              => new SapReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<SapReader>()),
            "shopifyplus"      => new ShopifyPlusReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<ShopifyPlusReader>()),
            "microsoftads"     => new MicrosoftAdsReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<MicrosoftAdsReader>()),
            "confluence"       => new ConfluenceReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<ConfluenceReader>()),
            "salesloft"        => new SalesloftReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<SalesloftReader>()),
            "googledrive"      => new GoogleDriveReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<GoogleDriveReader>()),
            "sentry"           => new SentryReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<SentryReader>()),
            "okta"             => new OktaReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<OktaReader>()),
            "firebase"         => new FirebaseReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<FirebaseReader>()),
            // Tier 1 — storage & communication
            "gcs"              => new GcsReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<GcsReader>()),
            "azureblob"        => new AzureBlobReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<AzureBlobReader>()),
            "sftp"             => new SftpReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<SftpReader>()),
            "slack"            => new SlackReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<SlackReader>()),
            "teams"            => new TeamsReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<TeamsReader>()),
            "quickbooks"       => new QuickBooksReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<QuickBooksReader>()),
            "xero"             => new XeroReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<XeroReader>()),
            "woocommerce"      => new WooCommerceReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<WooCommerceReader>()),
            // Tier 2 — marketing, analytics, SaaS
            "tiktokads"        => new TikTokAdsReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<TikTokAdsReader>()),
            "googlesearchconsole" => new GoogleSearchConsoleReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<GoogleSearchConsoleReader>()),
            "intercom"         => new IntercomReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<IntercomReader>()),
            "asana"            => new AsanaReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<AsanaReader>()),
            "monday"           => new MondayReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<MondayReader>()),
            "linear"           => new LinearReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<LinearReader>()),
            "notion"           => new NotionReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<NotionReader>()),
            "sendgrid"         => new SendGridReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<SendGridReader>()),
            "twilio"           => new TwilioReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<TwilioReader>()),
            "mailchimp"        => new MailchimpReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<MailchimpReader>()),
            "paypal"           => new PayPalReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<PayPalReader>()),
            "square"           => new SquareReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<SquareReader>()),
            "bigcommerce"      => new BigCommerceReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<BigCommerceReader>()),
            "mixpanel"         => new MixpanelReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<MixpanelReader>()),
            "amplitude"        => new AmplitudeReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<AmplitudeReader>()),
            "segment"          => new SegmentReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<SegmentReader>()),
            "klaviyo"          => new KlaviyoReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<KlaviyoReader>()),
            "marketo"          => new MarketoReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<MarketoReader>()),
            "dynamics365"      => new Dynamics365Reader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<Dynamics365Reader>()),
            "servicenow"       => new ServiceNowReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<ServiceNowReader>()),
            "freshdesk"        => new FreshdeskReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<FreshdeskReader>()),
            "zohocrm"          => new ZohoCrmReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<ZohoCrmReader>()),
            "netsuite"         => new NetSuiteReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<NetSuiteReader>()),
            "chargebee"        => new ChargebeeReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<ChargebeeReader>()),
            "snowplow"         => new SnowplowReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<SnowplowReader>()),
            // Tier 3 — social, dev tools, enterprise
            "instagram"        => new InstagramReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<InstagramReader>()),
            "youtube"          => new YouTubeReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<YouTubeReader>()),
            "twitter"          => new TwitterReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<TwitterReader>()),
            "reddit"           => new RedditReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<RedditReader>()),
            "magento"          => new MagentoReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<MagentoReader>()),
            "pardot"           => new PardotReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<PardotReader>()),
            "brevo"            => new BrevoReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<BrevoReader>()),
            "apollo"           => new ApolloReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<ApolloReader>()),
            "outreach"         => new OutreachReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<OutreachReader>()),
            "recurly"          => new RecurlyReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<RecurlyReader>()),
            "zuora"            => new ZuoraReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<ZuoraReader>()),
            "workday"          => new WorkdayReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<WorkdayReader>()),
            "bamboohr"         => new BambooHRReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<BambooHRReader>()),
            "gusto"            => new GustoReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<GustoReader>()),
            "gitlab"           => new GitLabReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<GitLabReader>()),
            "bitbucket"        => new BitbucketReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<BitbucketReader>()),
            "bingads"          => new BingAdsReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<BingAdsReader>()),
            "pinterestads"     => new PinterestAdsReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<PinterestAdsReader>()),
            "snapchatads"      => new SnapchatAdsReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<SnapchatAdsReader>()),
            "salesforcemarketingcloud" => new SalesforceMarketingCloudReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<SalesforceMarketingCloudReader>()),
            "sharepoint"       => new SharePointReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<SharePointReader>()),
            "elasticsearch"    => new ElasticsearchReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<ElasticsearchReader>()),
            "dynamodb"         => new DynamoDbReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<DynamoDbReader>()),
            "redis"            => new RedisReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<RedisReader>()),
            // Tier 4 — niche CRM, HR, dev, monitoring
            "copper"           => new CopperReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<CopperReader>()),
            "close"            => new CloseReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<CloseReader>()),
            "freshsales"       => new FreshsalesReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<FreshsalesReader>()),
            "harvest"          => new HarvestReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<HarvestReader>()),
            "toggl"            => new TogglReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<TogglReader>()),
            "greenhouse"       => new GreenhouseReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<GreenhouseReader>()),
            "lever"            => new LeverReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<LeverReader>()),
            "webflow"          => new WebflowReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<WebflowReader>()),
            "typeform"         => new TypeformReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<TypeformReader>()),
            "surveymonkey"     => new SurveyMonkeyReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<SurveyMonkeyReader>()),
            "datadog"          => new DatadogReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<DatadogReader>()),
            "pagerduty"        => new PagerDutyReader(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<PagerDutyReader>()),
            _                  => throw new NotSupportedException($"Source type '{type}' is not supported.")
        };

        // ── Destination Writers ───────────────────────────────────────────────

        public IDestinationWriter CreateDestinationWriter(string type) => type.ToLowerInvariant() switch
        {
            "webhook"    => new WebhookDestinationWriter(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<WebhookDestinationWriter>()),
            "sqlserver"  => new RelationalDbWriter("sqlserver",  _loggerFactory.CreateLogger<RelationalDbWriter>()),
            "postgresql" => new RelationalDbWriter("postgresql", _loggerFactory.CreateLogger<RelationalDbWriter>()),
            "mysql"      => new RelationalDbWriter("mysql",      _loggerFactory.CreateLogger<RelationalDbWriter>()),
            "oracle"     => new RelationalDbWriter("oracle",     _loggerFactory.CreateLogger<RelationalDbWriter>()),
            "mongodb"    => new MongoDbWriter(_loggerFactory.CreateLogger<MongoDbWriter>()),
            "neo4j"      => new Neo4jWriter(_loggerFactory.CreateLogger<Neo4jWriter>()),
            "snowflake"  => new SnowflakeWriter(_loggerFactory.CreateLogger<SnowflakeWriter>()),
            "bigquery"   => new BigQueryWriter(_loggerFactory.CreateLogger<BigQueryWriter>()),
            "pinecone"   => new PineconeWriter(_loggerFactory.CreateLogger<PineconeWriter>()),
            "milvus"     => new MilvusWriter(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<MilvusWriter>()),
            "shopify"      => new ShopifyWriter(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<ShopifyWriter>()),
            "googlesheets" => new GoogleSheetsWriter(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<GoogleSheetsWriter>()),
            "s3"           => new S3Writer(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<S3Writer>()),
            "airtable"     => new AirtableWriter(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<AirtableWriter>()),
            "gcs"          => new GcsWriter(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<GcsWriter>()),
            "azureblob"    => new AzureBlobWriter(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<AzureBlobWriter>()),
            "sftp"         => new SftpWriter(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<SftpWriter>()),
            "slack"        => new SlackWriter(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<SlackWriter>()),
            "teams"        => new TeamsWriter(_httpClientFactory.CreateClient(), _loggerFactory.CreateLogger<TeamsWriter>()),
            _              => throw new NotSupportedException($"Destination type '{type}' is not supported.")
        };

        // ── Connection Test ───────────────────────────────────────────────────

        public async Task<ConnectionTestResult> TestConnectionAsync(string provider, string connectionString)
        {
            var sw = Stopwatch.StartNew();
            try
            {
                await OpenAndCloseAsync(provider, connectionString);
                sw.Stop();
                return new ConnectionTestResult { Success = true, ElapsedMs = sw.ElapsedMilliseconds };
            }
            catch (Exception ex)
            {
                sw.Stop();
                return new ConnectionTestResult
                {
                    Success      = false,
                    ErrorMessage = ex.Message,
                    ElapsedMs    = sw.ElapsedMilliseconds,
                };
            }
        }

        private static async Task OpenAndCloseAsync(string provider, string connectionString)
        {
            switch (provider.ToLowerInvariant())
            {
                case "csv":
                    if (!System.IO.File.Exists(connectionString))
                        throw new System.IO.FileNotFoundException($"CSV file not found: {connectionString}");
                    break;
                case "rest":
                case "webhook":
                    using (var http = new System.Net.Http.HttpClient())
                    {
                        var resp = await http.GetAsync(connectionString);
                        resp.EnsureSuccessStatusCode();
                    }
                    break;
                case "sqlserver":
                    await using (var conn = new SqlConnection(connectionString)) { await conn.OpenAsync(); }
                    break;
                case "postgresql":
                    await using (var conn = new NpgsqlConnection(connectionString)) { await conn.OpenAsync(); }
                    break;
                case "mysql":
                    await using (var conn = new MySqlConnection(connectionString)) { await conn.OpenAsync(); }
                    break;
                case "oracle":
                    await using (var conn = new OracleConnection(connectionString)) { await conn.OpenAsync(); }
                    break;
                case "snowflake":
                    await using (var conn = new SnowflakeDbConnection())
                    {
                        conn.ConnectionString = connectionString;
                        await conn.OpenAsync();
                    }
                    break;
                case "mongodb":
                    var mongoClient = new MongoDB.Driver.MongoClient(connectionString);
                    await mongoClient.ListDatabaseNamesAsync();
                    break;
                case "neo4j":
                {
                    var opts = JsonSerializer.Deserialize<Neo4jTestOpts>(connectionString)
                               ?? throw new InvalidOperationException("Invalid Neo4j connection string.");
                    await using var driver = GraphDatabase.Driver(opts.Uri, AuthTokens.Basic(opts.User, opts.Password));
                    await driver.VerifyConnectivityAsync();
                    break;
                }
                case "bigquery":
                {
                    var opts = JsonSerializer.Deserialize<BqTestOpts>(connectionString)
                               ?? throw new InvalidOperationException("Invalid BigQuery connection string.");
                    var credential = Google.Apis.Auth.OAuth2.GoogleCredential.FromJson(opts.ServiceAccountJson)
                        .CreateScoped("https://www.googleapis.com/auth/bigquery");
                    var client = Google.Cloud.BigQuery.V2.BigQueryClient.Create(opts.ProjectId, credential);
                    await client.GetDatasetAsync(opts.Dataset);
                    break;
                }
                case "pinecone":
                {
                    var opts = JsonSerializer.Deserialize<PcTestOpts>(connectionString)
                               ?? throw new InvalidOperationException("Invalid Pinecone connection string.");
                    var client = new PineconeClient(opts.ApiKey);
                    await client.ListIndexesAsync();
                    break;
                }
                case "milvus":
                {
                    var opts = JsonSerializer.Deserialize<MilvusTestOpts>(connectionString)
                               ?? throw new InvalidOperationException("Invalid Milvus connection string.");
                    using var http = new System.Net.Http.HttpClient();
                    var resp = await http.GetAsync($"http://{opts.Host}:{opts.Port}/healthz");
                    resp.EnsureSuccessStatusCode();
                    break;
                }
                case "googlesheets":
                {
                    // connectionString is JSON: { "spreadsheetId", "apiKey", "accessToken" }
                    var gsOpts = JsonSerializer.Deserialize<GSheetsTestOpts>(connectionString)
                                 ?? throw new InvalidOperationException("Invalid Google Sheets connection string.");
                    using var gsHttp = new System.Net.Http.HttpClient();
                    var gsUrl = $"https://sheets.googleapis.com/v4/spreadsheets/{Uri.EscapeDataString(gsOpts.SpreadsheetId ?? "")}?fields=spreadsheetId";
                    if (!string.IsNullOrEmpty(gsOpts.AccessToken))
                        gsHttp.DefaultRequestHeaders.Authorization =
                            new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", gsOpts.AccessToken);
                    else if (!string.IsNullOrEmpty(gsOpts.ApiKey))
                        gsUrl += $"&key={Uri.EscapeDataString(gsOpts.ApiKey)}";
                    var gsResp = await gsHttp.GetAsync(gsUrl);
                    gsResp.EnsureSuccessStatusCode();
                    break;
                }
                case "shopify":
                {
                    // connectionString is JSON: { "shopDomain", "accessToken" }
                    var shopOpts = JsonSerializer.Deserialize<ShopifyTestOpts>(connectionString)
                                   ?? throw new InvalidOperationException("Invalid Shopify connection string.");
                    using var http = new System.Net.Http.HttpClient();
                    var shopifyUrl = $"https://{shopOpts.ShopDomain}/admin/api/2024-10/shop.json";
                    if (!string.IsNullOrEmpty(shopOpts.AccessToken))
                        http.DefaultRequestHeaders.Add("X-Shopify-Access-Token", shopOpts.AccessToken);
                    var resp = await http.GetAsync(shopifyUrl);
                    // 401 means shop exists but token is missing/wrong — domain is valid
                    if (resp.StatusCode != System.Net.HttpStatusCode.Unauthorized)
                        resp.EnsureSuccessStatusCode();
                    break;
                }
                case "hubspot":
                {
                    // connectionString is treated as the access token for connection testing.
                    // We call the HubSpot account-info endpoint to verify the token is valid.
                    using var hsHttp = new System.Net.Http.HttpClient();
                    hsHttp.DefaultRequestHeaders.Authorization =
                        new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", connectionString);
                    var hsResp = await hsHttp.GetAsync("https://api.hubapi.com/crm/v3/objects/contacts?limit=1");
                    hsResp.EnsureSuccessStatusCode();
                    break;
                }
                case "stripe":
                {
                    // connectionString is treated as the Stripe API key (sk_...)
                    using var stripeHttp = new System.Net.Http.HttpClient();
                    stripeHttp.DefaultRequestHeaders.Authorization =
                        new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", connectionString);
                    var stripeResp = await stripeHttp.GetAsync("https://api.stripe.com/v1/balance");
                    stripeResp.EnsureSuccessStatusCode();
                    break;
                }
                case "salesforce":
                {
                    // connectionString is JSON: { "instanceUrl", "accessToken" }
                    var sfOpts = JsonSerializer.Deserialize<SalesforceTestOpts>(connectionString)
                                 ?? throw new InvalidOperationException("Invalid Salesforce connection string.");
                    using var sfHttp = new System.Net.Http.HttpClient();
                    sfHttp.DefaultRequestHeaders.Authorization =
                        new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", sfOpts.AccessToken ?? "");
                    var sfUrl = $"{(sfOpts.InstanceUrl ?? "").TrimEnd('/')}/services/data/v59.0/sobjects/";
                    var sfResp = await sfHttp.GetAsync(sfUrl);
                    sfResp.EnsureSuccessStatusCode();
                    break;
                }
                case "zendesk":
                {
                    // connectionString is JSON: {"subdomain":"...","accessToken":"...","email":"..."}
                    var zdOpts = JsonSerializer.Deserialize<ZendeskTestOpts>(connectionString)
                                 ?? throw new InvalidOperationException("Invalid Zendesk connection string.");
                    using var zdHttp = new System.Net.Http.HttpClient();
                    var zdUrl = $"https://{zdOpts.Subdomain}.zendesk.com/api/v2/tickets.json?page[size]=1";
                    if (!string.IsNullOrEmpty(zdOpts.Email))
                    {
                        var credentials = $"{zdOpts.Email}/token:{zdOpts.AccessToken}";
                        var encoded = Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes(credentials));
                        zdHttp.DefaultRequestHeaders.Authorization =
                            new System.Net.Http.Headers.AuthenticationHeaderValue("Basic", encoded);
                    }
                    else
                    {
                        zdHttp.DefaultRequestHeaders.Authorization =
                            new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", zdOpts.AccessToken ?? "");
                    }
                    var zdResp = await zdHttp.GetAsync(zdUrl);
                    zdResp.EnsureSuccessStatusCode();
                    break;
                }
                case "pipedrive":
                {
                    // connectionString is the API token string
                    using var pdHttp = new System.Net.Http.HttpClient();
                    var pdUrl = $"https://api.pipedrive.com/v1/users?limit=1&api_token={Uri.EscapeDataString(connectionString)}";
                    var pdResp = await pdHttp.GetAsync(pdUrl);
                    pdResp.EnsureSuccessStatusCode();
                    break;
                }
                case "jira":
                {
                    // connectionString is JSON: {"domain":"...","email":"...","accessToken":"..."}
                    var jiraOpts = JsonSerializer.Deserialize<JiraTestOpts>(connectionString)
                                   ?? throw new InvalidOperationException("Invalid Jira connection string.");
                    using var jiraHttp = new System.Net.Http.HttpClient();
                    var jiraUrl = $"https://{jiraOpts.Domain}.atlassian.net/rest/api/3/myself";
                    if (!string.IsNullOrEmpty(jiraOpts.Email))
                    {
                        var credentials = Convert.ToBase64String(
                            System.Text.Encoding.UTF8.GetBytes($"{jiraOpts.Email}:{jiraOpts.AccessToken}"));
                        jiraHttp.DefaultRequestHeaders.Authorization =
                            new System.Net.Http.Headers.AuthenticationHeaderValue("Basic", credentials);
                    }
                    else
                    {
                        jiraHttp.DefaultRequestHeaders.Authorization =
                            new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", jiraOpts.AccessToken ?? "");
                    }
                    var jiraResp = await jiraHttp.GetAsync(jiraUrl);
                    jiraResp.EnsureSuccessStatusCode();
                    break;
                }
                case "github":
                {
                    // connectionString is the Personal Access Token (PAT)
                    using var ghHttp = new System.Net.Http.HttpClient();
                    ghHttp.DefaultRequestHeaders.Authorization =
                        new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", connectionString);
                    ghHttp.DefaultRequestHeaders.Accept.Add(
                        new System.Net.Http.Headers.MediaTypeWithQualityHeaderValue("application/vnd.github+json"));
                    ghHttp.DefaultRequestHeaders.TryAddWithoutValidation("User-Agent", "LoomPipe-Connector");
                    var ghResp = await ghHttp.GetAsync("https://api.github.com/user");
                    ghResp.EnsureSuccessStatusCode();
                    break;
                }
                case "airtable":
                {
                    // connectionString is the Personal Access Token or API key
                    using var atHttp = new System.Net.Http.HttpClient();
                    atHttp.DefaultRequestHeaders.Authorization =
                        new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", connectionString);
                    var atResp = await atHttp.GetAsync("https://api.airtable.com/v0/meta/whoami");
                    atResp.EnsureSuccessStatusCode();
                    break;
                }
                case "s3":
                {
                    // connectionString is JSON: { "Bucket", "AccessKeyId", "SecretAccessKey", "Region", "EndpointUrl" }
                    var opts = JsonSerializer.Deserialize<S3TestOpts>(connectionString)
                               ?? throw new InvalidOperationException("Invalid S3 connection string.");
                    var s3Config = new AmazonS3Config
                    {
                        RegionEndpoint = Amazon.RegionEndpoint.GetBySystemName(opts.Region ?? "us-east-1")
                    };
                    if (!string.IsNullOrEmpty(opts.EndpointUrl))
                    {
                        s3Config.ServiceURL = opts.EndpointUrl;
                        s3Config.ForcePathStyle = true;
                    }
                    using var s3Client = new AmazonS3Client(opts.AccessKeyId ?? "", opts.SecretAccessKey ?? "", s3Config);
                    await s3Client.ListObjectsV2Async(new ListObjectsV2Request
                    {
                        BucketName = opts.Bucket ?? "",
                        MaxKeys = 1
                    });
                    break;
                }
                case "cassandra":
                {
                    // connectionString is JSON: {"host":"...","port":9042,"keyspace":"...","username":"...","password":"..."}
                    var opts = JsonSerializer.Deserialize<CassandraTestOpts>(connectionString)
                               ?? throw new InvalidOperationException("Invalid Cassandra connection string.");
                    var builder = Cassandra.Cluster.Builder()
                        .AddContactPoint(opts.Host ?? "localhost")
                        .WithPort(opts.Port ?? 9042);
                    if (!string.IsNullOrEmpty(opts.Username) && !string.IsNullOrEmpty(opts.Password))
                        builder = builder.WithCredentials(opts.Username, opts.Password);
                    using var cassCluster = builder.Build();
                    using var cassSession = cassCluster.Connect(opts.Keyspace ?? "system");
                    cassSession.Execute("SELECT release_version FROM system.local");
                    break;
                }
                case "clickhouse":
                {
                    // connectionString is JSON: {"host":"...","port":8123,"database":"...","username":"...","password":"..."}
                    var opts = JsonSerializer.Deserialize<ClickHouseTestOpts>(connectionString)
                               ?? throw new InvalidOperationException("Invalid ClickHouse connection string.");
                    var chConnStr = $"Host={opts.Host ?? "localhost"};Port={opts.Port ?? 8123};Database={opts.Database ?? "default"};Username={opts.Username ?? "default"};Password={opts.Password ?? ""}";
                    await using (var chConn = new ClickHouse.Client.ADO.ClickHouseConnection(chConnStr))
                    {
                        await chConn.OpenAsync();
                    }
                    break;
                }
                case "databricks":
                {
                    // connectionString is JSON: {"accessToken":"...","host":"...","warehouseId":"..."}
                    var opts = JsonSerializer.Deserialize<DatabricksTestOpts>(connectionString)
                               ?? throw new InvalidOperationException("Invalid Databricks connection string.");
                    var dbHost = (opts.Host ?? "").TrimEnd('/');
                    if (!dbHost.StartsWith("http", StringComparison.OrdinalIgnoreCase))
                        dbHost = $"https://{dbHost}";
                    using var dbHttp = new System.Net.Http.HttpClient();
                    dbHttp.DefaultRequestHeaders.Authorization =
                        new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", opts.AccessToken ?? "");
                    var dbResp = await dbHttp.GetAsync($"{dbHost}/api/2.0/sql/warehouses/{Uri.EscapeDataString(opts.WarehouseId ?? "")}");
                    dbResp.EnsureSuccessStatusCode();
                    break;
                }
                case "redshift":
                {
                    // connectionString is JSON: {"host":"...","port":5439,"database":"...","username":"...","password":"..."}
                    var opts = JsonSerializer.Deserialize<RedshiftTestOpts>(connectionString)
                               ?? throw new InvalidOperationException("Invalid Redshift connection string.");
                    var rsConnStr = $"Host={opts.Host ?? "localhost"};Port={opts.Port ?? 5439};Database={opts.Database ?? "dev"};Username={opts.Username ?? ""};Password={opts.Password ?? ""}";
                    await using (var rsConn = new NpgsqlConnection(rsConnStr))
                    {
                        await rsConn.OpenAsync();
                    }
                    break;
                }
                default:
                    throw new NotSupportedException($"Provider '{provider}' is not supported for connection testing.");
            }
        }

        // Private records for JSON deserialization in TestConnectionAsync
        private record Neo4jTestOpts(string Uri, string User, string Password);
        private record BqTestOpts(string ProjectId, string Dataset, string ServiceAccountJson);
        private record PcTestOpts(string ApiKey, string IndexName, string Environment);
        private record MilvusTestOpts(string Host, int Port, string Collection, string User, string Password);
        private record S3TestOpts(string? Bucket, string? AccessKeyId, string? SecretAccessKey, string? Region, string? EndpointUrl);
        private record ShopifyTestOpts(string? ShopDomain, string? AccessToken);
        private record GSheetsTestOpts(string? SpreadsheetId, string? ApiKey, string? AccessToken);
        private record SalesforceTestOpts(string? InstanceUrl, string? AccessToken);
        private record ZendeskTestOpts(string? Subdomain, string? AccessToken, string? Email);
        private record JiraTestOpts(string? Domain, string? Email, string? AccessToken);
        private record CassandraTestOpts(string? Host, int? Port, string? Keyspace, string? Username, string? Password);
        private record ClickHouseTestOpts(string? Host, int? Port, string? Database, string? Username, string? Password);
        private record DatabricksTestOpts(string? AccessToken, string? Host, string? WarehouseId);
        private record RedshiftTestOpts(string? Host, int? Port, string? Database, string? Username, string? Password);
    }
}
