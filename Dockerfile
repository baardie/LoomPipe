# ── Stage 1: Build the React frontend ────────────────────────────────────────
FROM node:22-alpine AS frontend
WORKDIR /app
COPY loompipe.client/package*.json ./
RUN npm ci --prefer-offline
COPY loompipe.client/ ./
RUN npm run build

# ── Stage 2: Build the .NET backend ──────────────────────────────────────────
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS backend
WORKDIR /src

# Copy only project files first so NuGet restore is a cached layer
COPY LoomPipe.Core/LoomPipe.Core.csproj           LoomPipe.Core/
COPY LoomPipe.Data/LoomPipe.Data.csproj           LoomPipe.Data/
COPY LoomPipe.Storage/LoomPipe.Storage.csproj     LoomPipe.Storage/
COPY LoomPipe.Services/LoomPipe.Services.csproj   LoomPipe.Services/
COPY LoomPipe.Connectors/LoomPipe.Connectors.csproj LoomPipe.Connectors/
COPY LoomPipe.Engine/LoomPipe.Engine.csproj       LoomPipe.Engine/
COPY LoomPipe.Workers/LoomPipe.Workers.csproj     LoomPipe.Workers/
COPY LoomPipe.Server/LoomPipe.Server.csproj       LoomPipe.Server/
RUN dotnet restore LoomPipe.Server/LoomPipe.Server.csproj

# Copy full source
COPY LoomPipe.Core/           LoomPipe.Core/
COPY LoomPipe.Data/           LoomPipe.Data/
COPY LoomPipe.Storage/        LoomPipe.Storage/
COPY LoomPipe.Services/       LoomPipe.Services/
COPY LoomPipe.Connectors/     LoomPipe.Connectors/
COPY LoomPipe.Engine/         LoomPipe.Engine/
COPY LoomPipe.Workers/        LoomPipe.Workers/
COPY LoomPipe.Server/         LoomPipe.Server/

# Publish — skip the MSBuild SPA target; we copy the pre-built frontend below
RUN dotnet publish LoomPipe.Server/LoomPipe.Server.csproj \
    -c Release \
    -o /app/publish \
    -p:SkipSpaPublish=true \
    --no-restore

# Inject the pre-built React app into the publish output
COPY --from=frontend /app/dist/ /app/publish/wwwroot/

# ── Stage 3: Runtime image ────────────────────────────────────────────────────
FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS runtime
WORKDIR /app

# curl is used by the HEALTHCHECK
RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=backend /app/publish ./

# Directories for data-protection keys and persistent SQLite data (if used)
RUN mkdir -p /app/keys /app/data && chmod 700 /app/keys

EXPOSE 8080

ENV ASPNETCORE_URLS=http://+:8080
ENV ASPNETCORE_ENVIRONMENT=Production

HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

ENTRYPOINT ["dotnet", "LoomPipe.Server.dll"]
