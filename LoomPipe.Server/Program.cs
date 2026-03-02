using System.IO;
using System.Text;
using LoomPipe.Connectors;
using LoomPipe.Core.Entities;
using LoomPipe.Core.Interfaces;
using LoomPipe.Data.Database;
using LoomPipe.Server.Auth;
using LoomPipe.Services;
using LoomPipe.Storage.Repositories;
using LoomPipe.Workers;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.DataProtection;
using Microsoft.EntityFrameworkCore;
using Microsoft.IdentityModel.Tokens;
using Microsoft.AspNetCore.RateLimiting;
using System.Threading.RateLimiting;

var builder = WebApplication.CreateBuilder(args);
// ── CORS ─────────────────────────────────────────────────────────────────────
var allowedOrigins = builder.Configuration.GetSection("Cors:AllowedOrigins").Get<string[]>();
builder.Services.AddCors(options =>
{
    options.AddPolicy("DevCors", policy =>
    {
        policy.AllowAnyOrigin()
              .AllowAnyMethod()
              .AllowAnyHeader();
    });
    options.AddPolicy("ProdCors", policy =>
    {
        if (allowedOrigins is { Length: > 0 })
            policy.WithOrigins(allowedOrigins)
                  .AllowAnyMethod()
                  .AllowAnyHeader()
                  .AllowCredentials();
        else
            policy.AllowAnyOrigin()
                  .AllowAnyMethod()
                  .AllowAnyHeader();
    });
});

// ── DbContexts ──────────────────────────────────────────────────────────────
var dbProvider = builder.Configuration["Database:Provider"] ?? "SqlServer";
var connStr    = builder.Configuration.GetConnectionString("DefaultConnection");

if (builder.Environment.IsEnvironment("Testing"))
{
    builder.Services.AddDbContext<LoomPipe.Storage.LoomPipeDbContext>(options =>
        options.UseInMemoryDatabase("test_storage"));
    builder.Services.AddDbContext<LoomPipeDbContext>(options =>
        options.UseInMemoryDatabase("test_data"));
}
else
{
    builder.Services.AddDbContext<LoomPipe.Storage.LoomPipeDbContext>(options => {
        _ = dbProvider switch {
            "Sqlite"     => options.UseSqlite(connStr),
            "PostgreSQL" => options.UseNpgsql(connStr),
            _            => options.UseSqlServer(connStr),
        };
        options.ConfigureWarnings(w => w.Ignore(Microsoft.EntityFrameworkCore.Diagnostics.RelationalEventId.PendingModelChangesWarning));
    });
    builder.Services.AddDbContext<LoomPipeDbContext>(options => {
        _ = dbProvider switch {
            "Sqlite"     => options.UseSqlite(connStr),
            "PostgreSQL" => options.UseNpgsql(connStr),
            _            => options.UseSqlServer(connStr),
        };
        options.ConfigureWarnings(w => w.Ignore(Microsoft.EntityFrameworkCore.Diagnostics.RelationalEventId.PendingModelChangesWarning));
    });
}

// ── JWT Authentication ───────────────────────────────────────────────────────
var jwtSection = builder.Configuration.GetSection("Jwt");
var secretKey  = jwtSection["SecretKey"] ?? string.Empty;
if (builder.Environment.IsEnvironment("Testing") && secretKey.Length < 32)
    secretKey = "test-only-secret-key-that-is-at-least-32-characters-long!!";
else if (secretKey.Length < 32)
    throw new InvalidOperationException(
        "JWT SecretKey must be at least 32 characters. " +
        "Set it via environment variable Jwt__SecretKey or in appsettings.json.");

builder.Services.AddAuthentication(JwtBearerDefaults.AuthenticationScheme)
    .AddJwtBearer(options =>
    {
        options.TokenValidationParameters = new TokenValidationParameters
        {
            ValidateIssuer           = true,
            ValidateAudience         = true,
            ValidateLifetime         = true,
            ValidateIssuerSigningKey = true,
            ValidIssuer              = jwtSection["Issuer"],
            ValidAudience            = jwtSection["Audience"],
            IssuerSigningKey         = new SymmetricSecurityKey(Encoding.UTF8.GetBytes(secretKey)),
        };
    })
    .AddScheme<AuthenticationSchemeOptions, ApiKeyAuthHandler>(ApiKeyAuthHandler.SchemeName, _ => { });

builder.Services.AddAuthorization(o =>
    o.DefaultPolicy = new AuthorizationPolicyBuilder(
            JwtBearerDefaults.AuthenticationScheme,
            ApiKeyAuthHandler.SchemeName)
        .RequireAuthenticatedUser()
        .Build());

// ── Application Services ─────────────────────────────────────────────────────
// Persist data-protection keys to a directory so they survive container restarts.
// Override with DataProtection__KeysPath env var (e.g., /app/keys in Docker).
var dpKeysPath = builder.Configuration["DataProtection:KeysPath"]
    ?? Path.Combine(AppContext.BaseDirectory, "keys");
Directory.CreateDirectory(dpKeysPath);
builder.Services.AddDataProtection()
    .PersistKeysToFileSystem(new DirectoryInfo(dpKeysPath));
builder.Services.AddScoped<IConnectorService, ConnectorService>();
builder.Services.AddScoped<LoomPipe.Storage.Interfaces.IPipelineRepository, LoomPipe.Storage.Repositories.PipelineRepository>();
builder.Services.AddScoped<LoomPipe.Storage.Interfaces.IDataSourceConfigRepository, LoomPipe.Storage.Repositories.DataSourceConfigRepository>();
builder.Services.AddScoped<LoomPipe.Storage.Interfaces.IConnectionProfileRepository, LoomPipe.Storage.Repositories.ConnectionProfileRepository>();
builder.Services.AddScoped<IConnectionProfileService, ConnectionProfileService>();
builder.Services.AddScoped<IConnectorFactory, ConnectorFactory>();
builder.Services.AddScoped<LoomPipe.Core.Interfaces.IAppUserRepository, LoomPipe.Storage.Repositories.AppUserRepository>();
builder.Services.AddScoped<LoomPipe.Core.Interfaces.IPipelineRunLogRepository, LoomPipe.Storage.Repositories.PipelineRunLogRepository>();
builder.Services.AddScoped<LoomPipe.Core.Interfaces.IUserConnectionPermissionRepository, LoomPipe.Storage.Repositories.UserConnectionPermissionRepository>();
builder.Services.AddScoped<LoomPipe.Storage.Interfaces.ISmtpSettingsRepository, LoomPipe.Storage.Repositories.SmtpSettingsRepository>();
builder.Services.AddScoped<LoomPipe.Storage.Interfaces.IApiKeyRepository, LoomPipe.Storage.Repositories.ApiKeyRepository>();
builder.Services.AddScoped<IEmailNotificationService, EmailNotificationService>();
builder.Services.AddScoped<LoomPipe.Core.Interfaces.INotificationRepository, LoomPipe.Storage.Repositories.NotificationRepository>();
builder.Services.AddScoped<LoomPipe.Storage.Interfaces.ISystemSettingsRepository, LoomPipe.Storage.Repositories.SystemSettingsRepository>();
builder.Services.AddHostedService<ConnectorWorker>();
builder.Services.AddHttpClient();

// ── Rate Limiting ────────────────────────────────────────────────────────────
builder.Services.AddRateLimiter(options =>
{
    options.RejectionStatusCode = StatusCodes.Status429TooManyRequests;
    options.AddFixedWindowLimiter("auth", cfg =>
    {
        cfg.PermitLimit = 10;
        cfg.Window      = TimeSpan.FromMinutes(1);
        cfg.QueueLimit  = 0;
    });
});

builder.Services.AddControllers();
builder.Services.AddOpenApi();
builder.Services.AddSwaggerGen();


var app = builder.Build();

// Show detailed errors in development
if (app.Environment.IsDevelopment())
{
    app.UseDeveloperExceptionPage();
    app.UseCors("DevCors");
}

app.UseDefaultFiles();
app.UseStaticFiles();

// ── Migrate databases and seed default admin user ────────────────────────────
if (!app.Environment.IsEnvironment("Testing"))
{
    using var scope = app.Services.CreateScope();
    var logger = scope.ServiceProvider.GetRequiredService<ILogger<Program>>();

    // Apply EF Core migrations automatically on startup.
    // For the Data context (Connectors/Transformers schema), SQLite requires EnsureCreated()
    // because its migration uses SQL Server types. EnsureCreated() must run first so the
    // DB file doesn't already exist when called. Storage context uses Migrate() always.
    try
    {
        var dataCtx = scope.ServiceProvider.GetRequiredService<LoomPipeDbContext>();
        if (dbProvider == "Sqlite")
            dataCtx.Database.EnsureCreated();
        else
            dataCtx.Database.Migrate();

        scope.ServiceProvider.GetRequiredService<LoomPipe.Storage.LoomPipeDbContext>()
             .Database.Migrate();
    }
    catch (Exception ex)
    {
        logger.LogError(ex, "Database migration failed.");
    }

    // Seed a default admin account if the user table is empty.
    // Generate a random password and log it once so the operator can change it.
    var userRepo = scope.ServiceProvider.GetRequiredService<IAppUserRepository>();
    if (!await userRepo.AnyAsync())
    {
        var initialPassword = Convert.ToBase64String(
            System.Security.Cryptography.RandomNumberGenerator.GetBytes(18)); // 24-char random password
        await userRepo.AddAsync(new AppUser
        {
            Username     = "admin",
            PasswordHash = BCrypt.Net.BCrypt.HashPassword(initialPassword),
            Role         = "Admin",
            CreatedAt    = DateTime.UtcNow,
            IsActive     = true,
        });
        logger.LogWarning("Default admin account created. Username: admin  Password: {Password}  — change this immediately!", initialPassword);
    }
}

if (!app.Environment.IsProduction())
{
    app.MapOpenApi();
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseHttpsRedirection();

if (!app.Environment.IsDevelopment())
{
    app.UseHsts();
    app.UseCors("ProdCors");
}

app.UseRateLimiter();

app.UseAuthentication();
app.UseAuthorization();

app.MapControllers();

// Health check endpoint — used by Docker HEALTHCHECK and load-balancers
app.MapGet("/health", () => Results.Ok(new { status = "healthy" }))
   .AllowAnonymous()
   .ExcludeFromDescription();

app.MapFallbackToFile("index.html");

app.Run();

public partial class Program { }
