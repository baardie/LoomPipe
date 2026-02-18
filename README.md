# LoomPipe

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![.NET](https://img.shields.io/badge/.NET-10.0-512bd4?logo=dotnet)](https://dotnet.microsoft.com/download)
[![Tests](https://img.shields.io/badge/tests-23%20passing-brightgreen?logo=github)](#running-tests)
[![Buy Me a Coffee](https://img.shields.io/badge/donate-PayPal-blue?logo=paypal)](https://www.paypal.com/paypalme/baardie)

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

---

## Connectors

### Sources
| Connector | Type |
|---|---|
| CSV | File |
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
LoomPipe.Services       ← Application services (connection profiles, etc.)
LoomPipe.Workers        ← Background scheduler (ConnectorWorker)
    ↑
LoomPipe.Server         ← ASP.NET Core Web API, JWT auth, controllers
    ↑
loompipe.client         ← React + MUI + Vite frontend
```

### Tech Stack

**Backend**
- .NET 10 / ASP.NET Core Web API
- Entity Framework Core 10 + SQL Server (LocalDB for dev)
- JWT Bearer authentication (`Microsoft.AspNetCore.Authentication.JwtBearer`)
- BCrypt password hashing (`BCrypt.Net-Next`)
- ASP.NET Core Data Protection (AES-256-CBC credential encryption)

**Frontend**
- React 18 + Vite
- Material UI (MUI) v6

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
│   └── Interfaces/        # ISourceReader, IDestinationWriter, repositories, …
├── LoomPipe.Engine/       # PipelineEngine, TransformationParser, AutomapHelper
├── LoomPipe.Connectors/   # CSV, REST, SQL, MongoDB, Neo4j, Snowflake, BigQuery, Pinecone, Milvus, Webhook
├── LoomPipe.Storage/
│   ├── LoomPipeDbContext.cs
│   ├── Migrations/
│   └── Repositories/
├── LoomPipe.Services/     # ConnectionProfileService
├── LoomPipe.Workers/      # ConnectorWorker (background scheduler)
├── LoomPipe.Server/
│   ├── Controllers/       # Pipelines, Connections, Auth, Users, Analytics
│   └── Program.cs
├── loompipe.client/       # React + MUI frontend
│   └── src/
│       ├── pages/         # PipelineDetailPage, ProfileDetailPage, UsersPage, AnalyticsPage, …
│       ├── components/    # PipelineForm, PipelineList, ConnectionProfileDialog, …
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

---

## Support

If LoomPipe saved you from writing another bespoke ETL script at 2am, consider buying the developer a coffee — the pipelines run on caffeine too.

[![Buy Me a Coffee](https://img.shields.io/badge/donate-PayPal-blue?logo=paypal)](https://www.paypal.com/paypalme/baardie)

---

## License

MIT — made with ☕ by [Luke Baard](mailto:lukebaard@outlook.com)
