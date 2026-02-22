# LoomPipe

[![License: BSL 1.1](https://img.shields.io/badge/License-BSL_1.1-blue.svg)](LICENSE)
[![.NET](https://img.shields.io/badge/.NET-10.0-512bd4?logo=dotnet)](https://dotnet.microsoft.com/download)
[![Tests](https://img.shields.io/badge/tests-23%20passing-brightgreen?logo=github)](#running-tests)
[![Buy Me a Coffee](https://img.shields.io/badge/donate-PayPal-blue?logo=paypal)](https://www.paypal.com/paypalme/baardie)

## Want help?
I can integrate custom connectors, add new features and help you out! Send me an email at lukebaard@outlook.com.

---

**LoomPipe** is an open-source, self-hosted ETL (Extract, Transform, Load) platform with a web-based UI. Connect databases, APIs, files, and cloud data stores — then map, transform, and move data using an intuitive visual interface without writing code.

---

## Features

| | |
|---|---|
| **Automap-First field mapping** | Exact and fuzzy (Levenshtein) matching auto-maps source→destination fields in one click |
| **Expression transformations** | Inline expression language: literals, field references, concatenation, and built-in functions like `UPPER`, `LOWER`, `TRIM` |
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
| **File upload** | Upload CSV (up to 50 MB) or JSON (up to 100 MB) files directly through the UI as pipeline sources |
| **Multi-provider storage** | Run against SQLite (zero-config), PostgreSQL, or SQL Server — switch with a single config key |
| **Docker-ready** | Single-container Docker image using SQLite by default — no external database required |

---

## Connectors

### Sources
| Connector | Type |
|---|---|
| CSV | File |
| JSON | File / Inline |
| REST API | HTTP |
| SQL Server / PostgreSQL / MySQL / Oracle | Relational DB |
| MongoDB | Document DB |
| Neo4j | Graph DB |
| Snowflake | Cloud DW |
| BigQuery | Cloud DW |
| Pinecone | Vector DB |
| Milvus | Vector DB |

### Destinations
All relational, document, graph, cloud DW, and vector DB connectors above are also available as destinations, plus:

| Connector | Type |
|---|---|
| Webhook | HTTP POST |

---

## Architecture

LoomPipe follows Clean Architecture. Dependencies flow inward — only inner layers are referenced by outer ones.

```
LoomPipe.Core           ← Entities, interfaces, DTOs (no external deps)
    ↑
LoomPipe.Engine         ← Pipeline orchestration, automap, transformations
LoomPipe.Connectors     ← ISourceReader / IDestinationWriter implementations
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
- React 18 + Vite
- Tailwind CSS
- Lucide React icons

---

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

**SQLite (zero-config, recommended for local dev)**

Set the provider in `LoomPipe.Server/appsettings.json`:

```json
"Database": { "Provider": "Sqlite" },
"ConnectionStrings": {
  "DefaultConnection": "Data Source=loompipe.db"
}
```

Or pass it as environment variables:

```bash
Database__Provider=Sqlite
ConnectionStrings__DefaultConnection="Data Source=loompipe.db"
```

**SQL Server / LocalDB**

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

#### 3. Run the server

```bash
dotnet run --project LoomPipe.Server
```

Migrations are applied automatically on startup. The API starts at `https://localhost:5001` and serves the React frontend in development mode — no separate `npm run dev` needed.

#### 4. Install frontend dependencies (first run only)

```bash
cd loompipe.client && npm install
```

### Default credentials

On first startup, a default admin account is created:

| Username | Password |
|---|---|
| `admin` | `Admin123!` |

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

Transformations are defined one per line in the pipeline editor.

| Syntax | Example | Result |
|---|---|---|
| Literal assignment | `Country = 'US'` | Sets field to constant |
| Field copy | `FullName = Name` | Copies source field |
| Concatenation | `DisplayName = First + ' ' + Last` | Joins values |
| Function call | `Email = LOWER(Email)` | Applies built-in function |

**Built-in functions:** `UPPER`, `LOWER`, `TRIM`

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
# Engine (TransformationParser, AutomapHelper, PipelineEngine) — 12 tests
dotnet test tests/LoomPipe.Engine.Tests

# Connectors (CsvSourceReader, WebhookDestinationWriter) — 4 tests
dotnet test tests/LoomPipe.Connectors.Tests

# Core — 1 test
dotnet test tests/LoomPipe.Core.Tests

# Server integration (pipeline CRUD, auth) — 6 tests
dotnet test tests/LoomPipe.Server.Tests
```

All 23 tests use an in-memory database and a test auth handler — no real database or credentials required.

---

## Project Structure

```
LoomPipe/
├── LoomPipe.Core/
│   ├── Entities/          # Pipeline, AppUser, ApiKey, PipelineRunLog, UserConnectionPermission, …
│   ├── DTOs/              # Request/response DTOs
│   ├── Interfaces/        # ISourceReader, IDestinationWriter, IEmailNotificationService, repositories, …
│   └── Settings/          # EmailSettings
├── LoomPipe.Engine/       # PipelineEngine, TransformationParser, AutomapHelper
├── LoomPipe.Connectors/   # CSV, JSON, REST, SQL (with watermark support), MongoDB, Neo4j,
│                          # Snowflake, BigQuery, Pinecone, Milvus, Webhook
├── LoomPipe.Storage/
│   ├── LoomPipeDbContext.cs
│   ├── Migrations/        # SQLite-compatible EF Core migrations
│   └── Repositories/      # Pipeline, DataSourceConfig, ConnectionProfile, AppUser,
│                          # PipelineRunLog, UserConnectionPermission, SmtpSettings, ApiKey
├── LoomPipe.Services/     # ConnectionProfileService, EmailNotificationService
├── LoomPipe.Workers/      # ConnectorWorker (background cron scheduler + watermark advance)
├── LoomPipe.Server/
│   ├── Auth/              # ApiKeyAuthHandler (X-Api-Key scheme)
│   ├── Controllers/       # Pipelines, Connections, Auth, Users, Analytics,
│   │                      # AdminSettings, Csv, Json, ApiKeys
│   ├── appsettings.json             # Dev defaults (SqlServer)
│   ├── appsettings.Production.json  # Production defaults (Sqlite)
│   └── Program.cs         # Provider-switching DbContext, dual-scheme auth
├── loompipe.client/       # React + Tailwind frontend
│   └── src/
│       ├── pages/         # DashboardPage, PipelinesPage, PipelineDetailPage, ConnectionsPage,
│       │                  # ProfileDetailPage, UsersPage, AnalyticsPage, SettingsPage, LoginPage
│       ├── components/    # Sidebar, Topbar, ConfirmDialog, ErrorBoundary,
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
