# LoomPipe

[![License: BSL 1.1](https://img.shields.io/badge/License-BSL_1.1-blue.svg)](LICENSE)
[![.NET](https://img.shields.io/badge/.NET-10.0-512bd4?logo=dotnet)](https://dotnet.microsoft.com/download)
[![Tests](https://img.shields.io/badge/tests-93%20passing-brightgreen?logo=github)](#running-tests)
[![Buy Me a Coffee](https://img.shields.io/badge/donate-PayPal-blue?logo=paypal)](https://www.paypal.com/paypalme/baardie)

## Want help?
I can integrate custom connectors, add new features and help you out! Send me an email on [Linkdin](https://www.linkedin.com/in/lukebaard/).

---

**LoomPipe** is an open-source, self-hosted ETL (Extract, Transform, Load) platform with a web-based UI. Connect databases, APIs, files, and cloud data stores — then map, transform, and move data using an intuitive visual interface without writing code.

---

## Features

| | |
|---|---|
| **Automap-First field mapping** | Exact and fuzzy (Levenshtein) matching auto-maps source→destination fields in one click |
| **Expression transformations** | 48 built-in functions across 7 categories — string, numeric, type conversion, date/time, null/conditional, encoding, and hashing |
| **Dry-run preview** | Preview the first N rows of a pipeline before committing a full run |
| **Scheduled pipelines** | Cron expression scheduler — set any cron schedule and LoomPipe handles the rest |
| **Incremental / delta loads** | Watermark-based incremental loading — only pull records modified since the last run using a configurable timestamp or integer column |
| **Batch writing** | Configurable batch size and inter-batch delay to manage throughput and downstream load |
| **Connection profile vault** | Encrypted credential store (AES-256-CBC via ASP.NET Core Data Protection) — plaintext secrets never persisted |
| **API key authentication** | Generate long-lived API keys for CI/CD and programmatic pipeline triggers — presented in `X-Api-Key` header alongside JWT |
| **Role-based access control** | Three roles — Admin, User, Guest — with per-endpoint enforcement |
| **User-to-connection permissions** | Admins assign specific connection profiles to individual users |
| **Run history & analytics** | Per-pipeline run logs, duration, rows processed, error messages, and a cross-pipeline analytics dashboard |
| **Live connection testing** | Test any connection profile on-demand from the UI |
| **Dashboard & Live Monitor** | Real-time metric cards, visual pipeline canvas, live pipe monitor with auto-refresh, and source distribution chart |
| **Email notifications** | SMTP-based alerts on pipeline success or failure — configurable per-event with a test-send button |
| **In-app notification centre** | Real-time bell icon with unread badge, dropdown panel, type-tagged alerts for pipeline success and failure — click any notification to jump directly to the pipeline |
| **Failed run retry** | Re-run any failed pipeline run with one click — replays with the original config snapshot (stored securely for a configurable window, default 7 days) or falls back to current config if the snapshot has expired |
| **File upload** | Upload CSV (up to 50 MB) or JSON (up to 100 MB) files directly through the UI as pipeline sources |
| **Multi-provider storage** | Run against SQLite (zero-config), PostgreSQL, or SQL Server — switch with a single config key |
| **Docker-ready** | Single-container Docker image using SQLite by default — no external database required |

---

## Connectors

LoomPipe ships with **130 source connectors** and **22 destination connectors** spanning databases, SaaS platforms, cloud storage, analytics, and more.

### Sources by Category

| Category | Connectors |
|---|---|
| **Databases (17)** | SQL Server, PostgreSQL, MySQL, Oracle, MongoDB, Neo4j, Snowflake, BigQuery, Pinecone, Milvus, Elasticsearch, DynamoDB, Redis, Cassandra, ClickHouse, Databricks, Redshift |
| **CRM & Sales (8)** | Salesforce, HubSpot, Pipedrive, Zoho CRM, Dynamics 365, Copper, Close, Freshsales |
| **E-commerce (9)** | Stripe, Shopify, WooCommerce, BigCommerce, Magento, Shopify Plus, Salesforce Commerce Cloud, Square, PayPal |
| **Marketing & Ads (8)** | Google Ads, Facebook Ads, LinkedIn Ads, TikTok Ads, Bing Ads, Microsoft Ads, Pinterest Ads, Snapchat Ads |
| **Analytics (6)** | Google Analytics, Google Search Console, Mixpanel, Amplitude, Segment, Snowplow |
| **Communication (4)** | Slack, Microsoft Teams, Twilio, Intercom |
| **Project & Dev (9)** | Jira, GitHub, GitLab, Bitbucket, Asana, Monday.com, Linear, Notion, Confluence |
| **Customer Support (3)** | Zendesk, Freshdesk, ServiceNow |
| **Marketing Automation (10)** | Mailchimp, SendGrid, Klaviyo, Marketo, Pardot, Brevo, Salesforce Marketing Cloud, Salesloft, Outreach, Apollo.io |
| **Finance & Billing (8)** | QuickBooks, Xero, Chargebee, Recurly, Zuora, NetSuite, Harvest, Toggl |
| **Cloud Storage (7)** | Amazon S3, Google Cloud Storage, Azure Blob, SFTP, Google Drive, SharePoint, Google Sheets |
| **Social Media (4)** | Instagram, YouTube, Twitter/X, Reddit |
| **HR (5)** | Workday, BambooHR, Gusto, Greenhouse, Lever |
| **Productivity (4)** | Airtable, Webflow, Typeform, SurveyMonkey |
| **Monitoring (3)** | Datadog, PagerDuty, Sentry |
| **Enterprise (3)** | SAP, Okta, Firebase |
| **File Formats (4)** | CSV (upload up to 50 MB), JSON (inline paste or upload up to 100 MB), REST API, Webhook |

### Destinations (22)

| Connector | Type | Highlights |
|---|---|---|
| Webhook | HTTP POST | POST each record as JSON |
| SQL Server | Relational DB | Batch insert with auto-table creation |
| PostgreSQL | Relational DB | Batch insert with auto-table creation |
| MySQL | Relational DB | Batch insert with auto-table creation |
| Oracle | Relational DB | Batch insert with auto-table creation |
| MongoDB | Document DB | Collection insert (single or batch) |
| Neo4j | Graph DB | Cypher MERGE/CREATE |
| Snowflake | Cloud DW | Batch INSERT |
| BigQuery | Cloud DW | Streaming insert |
| Pinecone | Vector DB | Upsert vectors with metadata |
| Milvus | Vector DB | REST-based vector insert |
| Shopify | SaaS API | Create records via Admin REST API |
| Google Sheets | SaaS API | Append rows to spreadsheet |
| Amazon S3 | Cloud Storage | Write CSV/JSON/JSONL to S3 or MinIO |
| Google Cloud Storage | Cloud Storage | Write to GCS buckets |
| Azure Blob Storage | Cloud Storage | Write to Azure containers |
| SFTP | File Transfer | Upload files via SSH |
| Slack | Messaging | Post messages to channels |
| Microsoft Teams | Messaging | Post messages to channels |
| Airtable | SaaS API | Create/update records |
| WooCommerce | E-commerce | Create products/orders via REST API |
| BigCommerce | E-commerce | Create records via REST API |

> **Multi-resource connectors:** Shopify, Stripe, HubSpot, Google Sheets, and 80+ SaaS connectors support multiple endpoints within a single connector type. Select the specific resource via the `Parameters` field in the connection config.

---

## Architecture

LoomPipe follows Clean Architecture. Dependencies flow inward — only inner layers are referenced by outer ones.

```
LoomPipe.Core           ← Entities, interfaces, DTOs (no external deps)
    ↑
LoomPipe.Engine         ← Pipeline orchestration, automap, transformations
LoomPipe.Connectors     ← 130+ source/destination connector implementations
LoomPipe.Storage        ← EF Core repositories, DbContext, migrations
LoomPipe.Services       ← Application services (connection profiles, email notifications)
LoomPipe.Workers        ← Background scheduler (ConnectorWorker)
    ↑
LoomPipe.Server         ← ASP.NET Core Web API, JWT + API key auth, controllers
    ↑
loompipe.client         ← React + Vite frontend (Tailwind CSS)
```

### Tech Stack

**Backend**
- .NET 10 / ASP.NET Core Web API
- Entity Framework Core 10 with SQLite (default), PostgreSQL (Npgsql), or SQL Server
- JWT Bearer + API Key dual authentication
- BCrypt password hashing (`BCrypt.Net-Next`)
- ASP.NET Core Data Protection (AES-256-CBC credential encryption)
- SMTP email notifications (`System.Net.Mail`)

**Frontend**
- React 19 + Vite
- Tailwind CSS
- Lucide React icons

---

** Screenshots **

<img width="2559" height="1261" alt="image" src="https://github.com/user-attachments/assets/c2a428ba-04c3-4ac2-ae29-f59f44a3e103" />

<img width="2292" height="521" alt="image" src="https://github.com/user-attachments/assets/d4aa41fc-5eb2-47c9-a941-44cc73653181" />

<img width="491" height="455" alt="image" src="https://github.com/user-attachments/assets/d9ca254b-be7e-4f4a-b98f-01f8d432275a" />

<img width="2553" height="1261" alt="image" src="https://github.com/user-attachments/assets/740d3b65-75ca-40f1-b00d-3529b8b86e7e" />

<img width="735" height="1008" alt="image" src="https://github.com/user-attachments/assets/ffe637d3-9ee9-45a9-9f3a-161c063299fb" />

<img width="424" height="357" alt="image" src="https://github.com/user-attachments/assets/8b61a42e-c71e-44f0-b5b2-cbbaab0c6423" />

<img width="2556" height="480" alt="image" src="https://github.com/user-attachments/assets/6d32bac8-4b62-4af2-94d1-a817e95b5081" />

<img width="2553" height="1251" alt="image" src="https://github.com/user-attachments/assets/351e5dc5-7c38-458a-8ec6-9912be73da48" />


## Getting Started

### Quickest: Docker

```bash
git clone https://github.com/baardie/LoomPipe.git
cd LoomPipe
docker compose up --build
```

The app starts at `http://localhost:8080`. Data is persisted in a Docker volume — no external database needed.

### Local development

**Prerequisites**
- [.NET 10 SDK](https://dotnet.microsoft.com/download)
- [Node.js 20+](https://nodejs.org/)

#### 1. Clone

```bash
git clone https://github.com/baardie/LoomPipe.git
cd LoomPipe
```

#### 2. Choose a database

**SQLite (zero-config, default for local dev)**

`appsettings.Development.json` is pre-configured for SQLite — no changes needed. To override, edit that file:

```json
"Database": { "Provider": "Sqlite" },
"ConnectionStrings": {
  "DefaultConnection": "Data Source=loompipe.db"
}
```

**SQL Server / LocalDB**

Set in `LoomPipe.Server/appsettings.json` (or as environment variables):

```json
"Database": { "Provider": "SqlServer" },
"ConnectionStrings": {
  "DefaultConnection": "Server=(localdb)\\mssqllocaldb;Database=LoomPipe;Trusted_Connection=True;"
}
```

**PostgreSQL**

```json
"Database": { "Provider": "PostgreSQL" },
"ConnectionStrings": {
  "DefaultConnection": "Host=localhost;Database=loompipe;Username=...;Password=..."
}
```

> **Note:** PostgreSQL requires provider-specific migrations to be generated first. See [PostgreSQL migrations](#postgresql-migrations).

#### 3. Run the backend

```bash
dotnet run --project LoomPipe.Server
```

Migrations are applied automatically on startup. The API starts at `http://localhost:5259`.

#### 4. Run the frontend

In a separate terminal:

```bash
cd loompipe.client && npm install  # first run only
npm run dev
```

The UI is served at `http://localhost:5173` and proxies API calls to the backend.

### Default credentials

On first startup, a default admin account (`admin`) is created with a **randomly generated password**. Check the server logs for the password:

```
warn: LoomPipe.Server[0]
      Default admin account created. Username: admin  Password: <random>  — change this immediately!
```

**Change this immediately in production.**

---

## Incremental / Delta Loads

LoomPipe supports watermark-based incremental loading so pipelines only pull records that changed since the last run — no full-table scans on every schedule tick.

**How it works**

1. In the pipeline editor, open the **Incremental Load** section in the bottom settings strip.
2. Enable it and enter the name of a timestamp or integer column (e.g., `updated_at`, `modified_date`, `sequence_id`).
3. On the first run the full table is read. After a successful run, LoomPipe records the run's start time as the new watermark.
4. On subsequent runs, the source query becomes `SELECT * FROM {table} WHERE {column} > {last_watermark}`.

**Supported sources:** any relational DB connector (SQL Server, PostgreSQL, MySQL, Oracle).

The current watermark value (`Last sync`) is displayed read-only in the pipeline editor so you can see exactly where the last successful run left off.

---

## API Key Authentication

LoomPipe supports two authentication methods — both work on all protected endpoints:

| Method | Header | Best for |
|---|---|---|
| JWT Bearer | `Authorization: Bearer <token>` | Interactive UI sessions |
| API Key | `X-Api-Key: <key>` | CI/CD, scripts, programmatic triggers |

### Generating a key

1. Go to **Settings → API Keys**.
2. Click **New Key**, enter a name and optional expiry date, then click **Generate**.
3. Copy the key shown — it is displayed **once** and never stored in plaintext (only a SHA-256 hash is kept).

### Using a key

```bash
# Trigger a pipeline run from CI
curl -X POST https://your-instance/api/pipelines/42/run \
     -H "X-Api-Key: <your-key>"

# List pipelines
curl https://your-instance/api/pipelines \
     -H "X-Api-Key: <your-key>"
```

Keys carry the same role as the user who created them. An Admin key has full access; a User key is subject to the same restrictions as that user in the UI.

Revoke keys at any time from **Settings → API Keys**.

---

## Role Matrix

| Action | Admin | User | Guest |
|---|:---:|:---:|:---:|
| View pipeline list | ✓ | ✓ | ✓ |
| View run history & analytics | ✓ | ✓ | |
| Trigger a pipeline run | ✓ | ✓ | |
| View assigned connection profiles | ✓ | ✓ | |
| Create / edit / delete pipelines | ✓ | | |
| Create / edit / delete connection profiles | ✓ | | |
| Manage users | ✓ | | |
| Configure schedules, batch & incremental settings | ✓ | | |
| Assign connection profiles to users | ✓ | | |
| Configure email notifications | ✓ | | |
| Manage API keys (own keys) | ✓ | ✓ | |

---

## Transformation Expression Language

Transformations are defined one per line in the pipeline editor. Functions can be nested: `Name = TRIM(UPPER(Name))`.

| Syntax | Example | Result |
|---|---|---|
| Literal assignment | `Country = 'US'` | Sets field to constant |
| Field copy | `FullName = Name` | Copies source field |
| Concatenation | `DisplayName = First + ' ' + Last` | Joins values |
| Function call | `Email = LOWER(Email)` | Applies built-in function |
| Nested functions | `Name = TRIM(UPPER(Name))` | Composes multiple functions |

### Built-in Functions (48)

| Category | Functions |
|---|---|
| **String** (20) | `UPPER`, `LOWER`, `TRIM`, `LTRIM`, `RTRIM`, `REPLACE`, `REGEX_REPLACE`, `REVERSE`, `LEFT`, `RIGHT`, `SUBSTRING`, `LEN`/`LENGTH`, `PAD_LEFT`, `PAD_RIGHT`, `SPLIT`, `NORMALIZE`, `TITLE_CASE`, `SLUG`, `CONCAT` |
| **Numeric** (6) | `ROUND`, `CEIL`/`CEILING`, `FLOOR`, `ABS`, `MOD` |
| **Type Conversion** (4) | `TO_INT`, `TO_FLOAT`, `TO_STRING`, `TO_BOOL` |
| **Date/Time** (8) | `NOW`, `TODAY`, `FORMAT_DATE`, `ADD_DAYS`, `DATE_DIFF`, `YEAR`, `MONTH`, `DAY` |
| **Null/Conditional** (3) | `COALESCE`, `DEFAULT`, `NULLIF` |
| **Encoding/Hashing** (6) | `MD5`, `SHA256`, `BASE64_ENCODE`, `BASE64_DECODE`, `URL_ENCODE`, `URL_DECODE` |

See the in-app **Documentation** page for full syntax details and examples.

---

## Email Notifications

LoomPipe can send SMTP email alerts when pipelines succeed or fail. Configuration is Admin-only via **Settings → Email Notifications**.

| Setting | Description |
|---|---|
| SMTP Host / Port | Your mail server address and port (e.g. `smtp.gmail.com`, `587`) |
| SSL / TLS | Enable STARTTLS — recommended for ports 465 and 587 |
| Username / Password | SMTP credentials — password is stored server-side and never returned to the browser |
| From Address / Name | The sender displayed in the email |
| Admin Notification Email | The recipient for all pipeline event emails |
| Notify on failure | Send an alert when any pipeline run fails |
| Notify on success | Send an alert when a pipeline run completes successfully |

Use the **Send Test Email** button to verify your SMTP config without running a pipeline.

---

## JSON Source Connector

The JSON connector supports two modes:

| Mode | How it works |
|---|---|
| **Inline** | Paste raw JSON directly into the connection string field — ideal for small static datasets |
| **File** | Provide a server-side file path to a `.json` file (use the **Upload JSON** button in the UI) |

The root JSON value may be an array of objects `[{...},...]` or a single object `{...}` (treated as a one-record array). Nested objects and arrays are serialised to a JSON string for flat pipeline processing.

Upload limit: **100 MB** per file.

---

## PostgreSQL Migrations

The default EF Core migrations use SQLite types and run automatically for `Sqlite` and `SqlServer` providers. PostgreSQL requires a separate migration set:

```bash
# Generate PostgreSQL-specific migrations
dotnet ef migrations add Init \
  --project LoomPipe.Storage \
  --startup-project LoomPipe.Server \
  --output-dir Migrations/PostgreSQL \
  -- --provider PostgreSQL
```

Then update `Program.cs` to use the PostgreSQL migration assembly if needed, or use `EnsureCreated` for simple setups.

---

## Running Tests

```bash
# Engine (TransformationParser, AutomapHelper, PipelineEngine) — 82 tests
dotnet test tests/LoomPipe.Engine.Tests

# Connectors (CsvSourceReader, WebhookDestinationWriter) — 4 tests
dotnet test tests/LoomPipe.Connectors.Tests

# Core — 1 test
dotnet test tests/LoomPipe.Core.Tests

# Server integration (pipeline CRUD, auth) — 6 tests
dotnet test tests/LoomPipe.Server.Tests
```

All 93 tests use an in-memory database and a test auth handler — no real database or credentials required.

---

## Project Structure

```
LoomPipe/
├── LoomPipe.Core/
│   ├── Entities/          # Pipeline, AppUser, ApiKey, PipelineRunLog, Notification, …
│   ├── DTOs/              # Request/response DTOs
│   ├── Interfaces/        # ISourceReader, IDestinationWriter, IEmailNotificationService, INotificationRepository, …
│   └── Settings/          # EmailSettings
├── LoomPipe.Engine/       # PipelineEngine, TransformationParser, AutomapHelper
├── LoomPipe.Connectors/   # 130 source readers + 22 destination writers
│                          # Databases, SaaS, cloud storage, analytics, and more
├── LoomPipe.Storage/
│   ├── LoomPipeDbContext.cs
│   ├── Migrations/        # SQLite-compatible EF Core migrations
│   └── Repositories/      # Pipeline, DataSourceConfig, ConnectionProfile, AppUser,
│                          # PipelineRunLog, UserConnectionPermission, SmtpSettings, ApiKey, Notification
├── LoomPipe.Services/     # ConnectionProfileService, EmailNotificationService
├── LoomPipe.Workers/      # ConnectorWorker (background cron scheduler + watermark advance)
├── LoomPipe.Server/
│   ├── Auth/              # ApiKeyAuthHandler (X-Api-Key scheme)
│   ├── Controllers/       # Pipelines, Connections, Auth, Users, Analytics,
│   │                      # AdminSettings, Csv, Json, ApiKeys, Notifications
│   ├── appsettings.json             # Base config (SqlServer)
│   ├── appsettings.Development.json # Dev overrides (SQLite, zero-config)
│   ├── appsettings.Production.json  # Production defaults (Sqlite)
│   └── Program.cs         # Provider-switching DbContext, dual-scheme auth
├── loompipe.client/       # React + Tailwind frontend
│   └── src/
│       ├── pages/         # DashboardPage, PipelinesPage, PipelineDetailPage, ConnectionsPage,
│       │                  # ProfileDetailPage, UsersPage, AnalyticsPage, SettingsPage,
│       │                  # DocumentationPage, LoginPage
│       ├── components/    # Sidebar, Topbar, NotificationCenter, ConfirmDialog, ErrorBoundary,
│       │                  # PipelineForm, PipelineList, ConnectionProfileDialog,
│       │                  # loom/ (LoomCanvas, LoomEditor, LoomPanel, LoomSettings),
│       │                  # pipeline/ (DraggableFieldMapping, DryRunResultModal, …)
│       └── contexts/      # AuthContext (JWT, authFetch, role helpers)
├── tests/
│   ├── LoomPipe.Engine.Tests/
│   ├── LoomPipe.Connectors.Tests/
│   ├── LoomPipe.Core.Tests/
│   └── LoomPipe.Server.Tests/
├── Dockerfile
└── docker-compose.yml     # Single-container SQLite deployment
```

---

## Security

- **Passwords** are hashed with BCrypt (work factor 11).
- **API keys** are shown once at creation and stored only as a SHA-256 hex hash. There is no way to recover a lost key — generate a new one and revoke the old one.
- **Connection secrets** (passwords, API keys, service account JSON) are encrypted with AES-256-CBC using ASP.NET Core Data Protection before being stored. Plaintext is never written to the database.
- **API endpoints** are protected with JWT Bearer tokens or `X-Api-Key` headers. Role restrictions are enforced server-side — client-side role guards are UI-only.
- **User-to-connection permissions** allow Admins to restrict which connection profiles each User-role account can see and use.
- **SMTP password** is stored server-side and never returned to the browser — the API only indicates whether one has been set.
- **Incremental watermark queries** use parameterized ADO.NET commands; the column name is validated against a strict `^[\w.]+$` regex to prevent SQL injection.

---

## Support

If LoomPipe saved you from writing another bespoke ETL script at 2am, consider buying the developer a coffee — the pipelines run on caffeine too.

[![Buy Me a Coffee](https://img.shields.io/badge/donate-PayPal-blue?logo=paypal)](https://www.paypal.com/paypalme/baardie)

---

## License

Business Source License 1.1 — see [LICENSE](LICENSE) for details.
Made with ☕ by [Luke Baard](mailto:lukebaard@outlook.com)
https://www.linkedin.com/in/lukebaard/
