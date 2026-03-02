# Contributing to LoomPipe

Thank you for your interest in contributing to LoomPipe! This guide will help you get started.

## Table of Contents

- [Development Environment Setup](#development-environment-setup)
- [Project Structure](#project-structure)
- [Adding a New Connector](#adding-a-new-connector)
- [Pull Request Guidelines](#pull-request-guidelines)
- [Coding Conventions](#coding-conventions)
- [Reporting Issues](#reporting-issues)

## Development Environment Setup

### Prerequisites

- [.NET 10 SDK](https://dotnet.microsoft.com/download)
- [Node.js 22+](https://nodejs.org/) (with npm)
- SQL Server (LocalDB or a full instance)

### Getting Started

1. Clone the repository:
   ```bash
   git clone https://github.com/your-org/LoomPipe.git
   cd LoomPipe
   ```

2. Build the backend:
   ```bash
   dotnet build
   ```

3. Set up the frontend:
   ```bash
   cd loompipe.client
   npm install
   npm run dev
   ```

4. Run the server:
   ```bash
   dotnet run --project LoomPipe.Server
   ```

5. Run the test suites:
   ```bash
   dotnet test tests/LoomPipe.Engine.Tests
   dotnet test tests/LoomPipe.Connectors.Tests
   dotnet test tests/LoomPipe.Core.Tests
   dotnet test tests/LoomPipe.Server.Tests
   ```

   > **Note:** Run each test project separately. Running `dotnet test` with multiple project paths in one command can fail.

### Default Credentials

On first startup, a default admin user (`admin`) is created with a random password. Check the server logs for the generated password and change it immediately.

## Project Structure

```
LoomPipe/
  LoomPipe.Core/          # Domain models, interfaces (ISourceReader, IDestinationWriter, IConnectorFactory)
  LoomPipe.Engine/         # Pipeline execution engine, transformation parser, automapping
  LoomPipe.Connectors/     # Connector implementations (CSV, SQL, Webhook, REST, etc.)
  LoomPipe.Storage/        # EF Core DbContext for pipelines, auth, run logs
  LoomPipe.Data/           # EF Core DbContext for connector profiles
  LoomPipe.Services/       # Business logic services
  LoomPipe.Workers/        # Background workers (ConnectorWorker for scheduled pipelines)
  LoomPipe.Server/         # ASP.NET Core Web API host, controllers, auth
  loompipe.client/         # React + MUI + Vite frontend
  tests/                   # Test projects (Engine, Connectors, Core, Server integration)
```

## Adding a New Connector

LoomPipe uses a factory pattern for connectors. To add a new source or destination:

1. **Implement the interface** in `LoomPipe.Connectors/`:

   For a source connector, implement `ISourceReader`:
   ```csharp
   public class MySourceReader : ISourceReader
   {
       public async Task<IEnumerable<Dictionary<string, object?>>> ReadAsync(
           ConnectionProfile profile, CancellationToken ct = default)
       {
           // Read and return rows from your data source
       }
   }
   ```

   For a destination connector, implement `IDestinationWriter`:
   ```csharp
   public class MyDestinationWriter : IDestinationWriter
   {
       public async Task WriteAsync(
           ConnectionProfile profile,
           IEnumerable<Dictionary<string, object?>> rows,
           CancellationToken ct = default)
       {
           // Write rows to your destination
       }
   }
   ```

2. **Register the connector** in `ConnectorFactory` so the DI container can resolve it. Add your new type to the appropriate switch/map in `ConnectorFactory.cs`.

3. **Write tests** in the appropriate test project under `tests/`.

> **Important:** Never instantiate connectors directly with `new`. Always use `IConnectorFactory` for DI-safe connector creation.

## Pull Request Guidelines

- **Keep PRs small and focused.** One logical change per PR makes review easier.
- **Run the tests** before submitting: `dotnet test` for each test project.
- **Describe your changes** clearly in the PR description. Explain *what* changed and *why*.
- **Reference related issues** (e.g., "Closes #42").
- **Include screenshots** for any UI changes.
- **Use the PR template** provided in this repository.

## Coding Conventions

### .NET / C#

- Follow standard [.NET naming conventions](https://learn.microsoft.com/en-us/dotnet/csharp/fundamentals/coding-style/coding-conventions) (PascalCase for public members, camelCase for locals).
- Use `async`/`await` consistently. Suffix async methods with `Async`.
- Never hard-code connector instantiation. Use `IConnectorFactory`.
- Wrap external service calls with proper error handling. Use `ConnectorException` (requires both message and inner exception).
- Keep controllers thin; push business logic into services.

### Frontend (React / TypeScript)

- Use functional components with hooks.
- Use `authFetch` (from `AuthContext`) for all API calls, not bare `fetch`.
- Follow the existing MUI component patterns.
- Use `RoleGuard` for role-based UI visibility.

## Reporting Issues

- Use the appropriate [issue template](https://github.com/your-org/LoomPipe/issues/new/choose) (Bug Report or Feature Request).
- Search existing issues first to avoid duplicates.
- For bug reports, include steps to reproduce, expected vs. actual behavior, and your environment details.
- For security vulnerabilities, please report them privately rather than opening a public issue.

## License

By contributing to LoomPipe, you agree that your contributions will be licensed under the project's [BSL 1.1 license](LICENSE), which transitions to MIT after the specified change date.
