# LoomPipe

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![.NET](https://img.shields.io/badge/.NET-10.0-512bd4?logo=dotnet)](https://dotnet.microsoft.com/download)
[![Tests](https://img.shields.io/badge/tests-23%20passing-brightgreen?logo=github)](#running-tests)
[![Buy Me a Coffee](https://img.shields.io/badge/donate-PayPal-blue?logo=paypal)](https://www.paypal.com/paypalme/baardie)

## Want help?
I can integrate custom connectors, add new features and help you out! Send me an email at lukebaard@outlook.com.

This is still a major work in progress, some things may be broken.

---


**LoomPipe** is an open-source, self-hosted ETL (Extract, Transform, Load) platform with a web-based UI. Connect databases, APIs, files, and cloud data stores — then map, transform, and move data using an intuitive interface without writing code.

---

## Features

| | |
|---|---|
| **Automap-First field mapping** | Exact and fuzzy (Levenshtein) matching auto-maps source→destination fields in one click |
| **Expression transformations** | Inline expression language: literals, field references, concatenation, and built-in functions like `UPPER`, `LOWER`, `TRIM` |
| **Dry-run preview** | Preview the first N rows of a pipeline before committing a full run |
| **Scheduled pipelines** | Built-in cron-style scheduler — set an interval in minutes and LoomPipe handles the rest |
| **Batch writing** | Configurable batch size and inter-batch delay to manage throughput and downstream load |
| **Connection profile vault** | Encrypted credential store (AES-256-CBC via ASP.NET Core Data Protection) — plaintext secrets never persisted |
| **Role-based access control** | Three roles — Admin, User, Guest — with per-endpoint enforcement |
| **User-to-connection permissions** | Admins assign specific connection profiles to individual users |
| **Run history & analytics** | Per-pipeline run logs, duration, rows processed, error messages, and a cross-pipeline analytics dashboard |
| **Live connection testing** | Test any connection profile on-demand from the UI |
| **Dashboard & Live Monitor** | Real-time metric cards, visual pipeline canvas, live pipe monitor with auto-refresh, and source distribution chart |
| **Email notifications** | SMTP-based alerts on pipeline success or failure — configurable per-event with a test-send button |
| **File upload** | Upload CSV (up to 50 MB) or JSON (up to 100 MB) files directly through the UI as pipeline sources |

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
LoomPipe.Server         ← ASP.NET Core Web API, JWT auth, controllers
    ↑
loompipe.client         ← React + Vite frontend (Tailwind CSS)
```

### Tech Stack

**Backend**
- .NET 10 / ASP.NET Core Web API
- Entity Framework Core 10 + SQL Server (LocalDB for dev)
- JWT Bearer authentication (`Microsoft.AspNetCore.Authentication.JwtBearer`)
- BCrypt password hashing (`BCrypt.Net-Next`)
- ASP.NET Core Data Protection (AES-256-CBC credential encryption)
- SMTP email notifications (`System.Net.Mail`)

**Frontend**
- React 18 + Vite
- Tailwind CSS
- Lucide React icons

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
| Configure schedules & batch settings | ✓ | | |
| Assign connection profiles to users | ✓ | | |
| Configure email notifications | ✓ | | |

---

## Getting Started

### Prerequisites

- [.NET 10 SDK](https://dotnet.microsoft.com/download)
- [Node.js 20+](https://nodejs.org/)
- SQL Server or [LocalDB](https://learn.microsoft.com/en-us/sql/database-engine/configure-windows/sql-server-express-localdb) (included with Visual Studio)

### 1. Clone

```bash
git clone https://github.com/baardie/LoomPipe.git
cd LoomPipe
```

### 2. Configure the database

Edit `LoomPipe.Server/appsettings.json` and set your connection string:

```json
"ConnectionStrings": {
  "DefaultConnection": "Server=(localdb)\\mssqllocaldb;Database=LoomPipe;Trusted_Connection=True;"
}
```

### 3. Apply migrations

```bash
dotnet ef database update --project LoomPipe.Storage --startup-project LoomPipe.Server
```

### 4. Run the server

```bash
dotnet run --project LoomPipe.Server
```

The API starts at `https://localhost:5001` and serves the React frontend via Vite proxy in development.

### 5. Install frontend dependencies (first run only)

```bash
cd loompipe.client
npm install
```

The frontend is automatically served by the .NET project in development mode — no separate `npm run dev` needed.

### Default credentials

On first startup, a default admin account is created:

| Username | Password |
|---|---|
| `admin` | `Admin123!` |

**Change this immediately in production.**

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

## Dashboard

The dashboard (`/`) provides a live overview of your LoomPipe instance:

- **Metric cards** — Active Pipelines, Total Runs, Success Rate, and Pipeline Errors at a glance.
- **Visual Loom Editor** — A canvas that renders your pipelines as source → destination node graphs with Bezier connector edges. Incomplete or misconfigured pipelines are shown with dashed amber edges.
- **Live Pipe Monitor** — A table of your most recent pipelines showing current status, rows processed, and last-run time. Auto-refreshes every 15 seconds.
- **Weave Distribution** — A bar chart breaking down your pipelines by source connector type.
- **Run Summary** — Quick stats panel showing totals, average duration, and success rate.

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

Settings are persisted to `email-settings.json` in the server content root. Keep this file out of source control.

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
│   ├── Entities/          # Pipeline, AppUser, PipelineRunLog, UserConnectionPermission, …
│   ├── DTOs/              # Request/response DTOs
│   ├── Interfaces/        # ISourceReader, IDestinationWriter, IEmailNotificationService, repositories, …
│   └── Settings/          # EmailSettings
├── LoomPipe.Engine/       # PipelineEngine, TransformationParser, AutomapHelper
├── LoomPipe.Connectors/   # CSV, JSON, REST, SQL, MongoDB, Neo4j, Snowflake, BigQuery, Pinecone, Milvus, Webhook
├── LoomPipe.Storage/
│   ├── LoomPipeDbContext.cs
│   ├── Migrations/
│   └── Repositories/
├── LoomPipe.Services/     # ConnectionProfileService, EmailNotificationService
├── LoomPipe.Workers/      # ConnectorWorker (background scheduler)
├── LoomPipe.Server/
│   ├── Controllers/       # Pipelines, Connections, Auth, Users, Analytics, AdminSettings, Csv, Json
│   └── Program.cs
├── loompipe.client/       # React + Tailwind frontend
│   └── src/
│       ├── pages/         # DashboardPage, PipelinesPage, PipelineDetailPage, ConnectionsPage,
│       │                  # ProfileDetailPage, UsersPage, AnalyticsPage, SettingsPage, LoginPage
│       ├── components/    # Sidebar, Topbar, ConfirmDialog, ErrorBoundary,
│       │                  # PipelineForm, PipelineList, ConnectionProfileDialog,
│       │                  # loom/ (LoomCanvas, LoomEditor, LoomPanel, LoomSettings),
│       │                  # pipeline/ (DraggableFieldMapping, DryRunResultModal, …)
│       └── contexts/      # AuthContext (JWT, authFetch, role helpers)
└── tests/
    ├── LoomPipe.Engine.Tests/
    ├── LoomPipe.Connectors.Tests/
    ├── LoomPipe.Core.Tests/
    └── LoomPipe.Server.Tests/
```

---

## Security

- **Passwords** are hashed with BCrypt (work factor 11).
- **Connection secrets** (passwords, API keys, service account JSON) are encrypted with AES-256-CBC using ASP.NET Core Data Protection before being stored. Plaintext is never written to the database.
- **API endpoints** are protected with JWT Bearer tokens. Role restrictions are enforced server-side — client-side role guards are UI-only.
- **User-to-connection permissions** allow Admins to restrict which connection profiles each User-role account can see and use.
- **SMTP password** is stored in `email-settings.json` on the server. The password is never returned to the browser — the API only indicates whether one has been set.

---

## Support

If LoomPipe saved you from writing another bespoke ETL script at 2am, consider buying the developer a coffee — the pipelines run on caffeine too.

[![Buy Me a Coffee](https://img.shields.io/badge/donate-PayPal-blue?logo=paypal)](https://www.paypal.com/paypalme/baardie)

---

## License

MIT — made with ☕ by [Luke Baard](mailto:lukebaard@outlook.com)
