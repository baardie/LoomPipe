using System.IO;
using System.Text;
using LoomPipe.Connectors;
using LoomPipe.Core.Entities;
using LoomPipe.Core.Interfaces;
using LoomPipe.Data.Database;
using LoomPipe.Services;
using LoomPipe.Storage.Repositories;
using LoomPipe.Workers;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.AspNetCore.DataProtection;
using Microsoft.EntityFrameworkCore;
using Microsoft.IdentityModel.Tokens;

var builder = WebApplication.CreateBuilder(args);
// ── CORS ─────────────────────────────────────────────────────────────────────
builder.Services.AddCors(options =>
{
    options.AddPolicy("DevCors", policy =>
    {
        policy.AllowAnyOrigin()
              .AllowAnyMethod()
              .AllowAnyHeader();
    });
});

// ── DbContexts ──────────────────────────────────────────────────────────────
if (builder.Environment.IsEnvironment("Testing"))
{
    builder.Services.AddDbContext<LoomPipe.Storage.LoomPipeDbContext>(options =>
        options.UseInMemoryDatabase("test_storage"));
    builder.Services.AddDbContext<LoomPipeDbContext>(options =>
        options.UseInMemoryDatabase("test_data"));
}
else
{
    builder.Services.AddDbContext<LoomPipe.Storage.LoomPipeDbContext>(options =>
        options.UseSqlServer(builder.Configuration.GetConnectionString("DefaultConnection")));
    builder.Services.AddDbContext<LoomPipeDbContext>(options =>
        options.UseSqlServer(builder.Configuration.GetConnectionString("DefaultConnection")));
}

// ── JWT Authentication ───────────────────────────────────────────────────────
var jwtSection = builder.Configuration.GetSection("Jwt");
var secretKey  = jwtSection["SecretKey"] ?? "CHANGE_ME_32_CHARS_MIN_SECRET_KEY!!";

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
    });

builder.Services.AddAuthorization();

// ── Application Services ─────────────────────────────────────────────────────
builder.Services.AddDataProtection();
builder.Services.AddScoped<IConnectorService, ConnectorService>();
builder.Services.AddScoped<LoomPipe.Storage.Interfaces.IPipelineRepository, LoomPipe.Storage.Repositories.PipelineRepository>();
builder.Services.AddScoped<LoomPipe.Storage.Interfaces.IDataSourceConfigRepository, LoomPipe.Storage.Repositories.DataSourceConfigRepository>();
builder.Services.AddScoped<LoomPipe.Storage.Interfaces.IConnectionProfileRepository, LoomPipe.Storage.Repositories.ConnectionProfileRepository>();
builder.Services.AddScoped<IConnectionProfileService, ConnectionProfileService>();
builder.Services.AddScoped<IConnectorFactory, ConnectorFactory>();
builder.Services.AddScoped<LoomPipe.Core.Interfaces.IAppUserRepository, LoomPipe.Storage.Repositories.AppUserRepository>();
builder.Services.AddScoped<LoomPipe.Core.Interfaces.IPipelineRunLogRepository, LoomPipe.Storage.Repositories.PipelineRunLogRepository>();
builder.Services.AddScoped<LoomPipe.Core.Interfaces.IUserConnectionPermissionRepository, LoomPipe.Storage.Repositories.UserConnectionPermissionRepository>();
builder.Services.AddHostedService<ConnectorWorker>();
builder.Services.AddHttpClient();

// ── Email notification service (singleton — reads/writes a settings file) ────
var emailSettingsPath = Path.Combine(builder.Environment.ContentRootPath, "email-settings.json");
builder.Services.AddSingleton<IEmailNotificationService>(sp =>
    new EmailNotificationService(
        sp.GetRequiredService<Microsoft.Extensions.Logging.ILogger<EmailNotificationService>>(),
        emailSettingsPath));

builder.Services.AddControllers();
builder.Services.AddOpenApi();


var app = builder.Build();

// Show detailed errors in development
if (app.Environment.IsDevelopment())
{
    app.UseDeveloperExceptionPage();
    app.UseCors("DevCors");
}

app.UseDefaultFiles();
app.UseStaticFiles();

// ── Seed default admin user ───────────────────────────────────────────────────
if (!app.Environment.IsEnvironment("Testing"))
{
    using var scope = app.Services.CreateScope();
    var userRepo = scope.ServiceProvider.GetRequiredService<IAppUserRepository>();
    if (!await userRepo.AnyAsync())
    {
        await userRepo.AddAsync(new AppUser
        {
            Username     = "admin",
            PasswordHash = BCrypt.Net.BCrypt.HashPassword("Admin123!"),
            Role         = "Admin",
            CreatedAt    = DateTime.UtcNow,
            IsActive     = true,
        });
    }
}

if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.UseHttpsRedirection();
// In production, configure CORS more restrictively

app.UseAuthentication();
app.UseAuthorization();

app.MapControllers();
app.MapFallbackToFile("index.html");

app.Run();

public partial class Program { }
