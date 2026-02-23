using LoomPipe.Core.Entities;
using Microsoft.EntityFrameworkCore;
using System.Text.Json;

namespace LoomPipe.Storage
{
    public class LoomPipeDbContext : DbContext
    {
        public LoomPipeDbContext(DbContextOptions<LoomPipeDbContext> options) : base(options) { }

        public DbSet<Pipeline> Pipelines { get; set; }
        public DbSet<DataSourceConfig> DataSourceConfigs { get; set; }
        public DbSet<FieldMap> FieldMaps { get; set; }
        public DbSet<ConnectionProfile> ConnectionProfiles { get; set; }
        public DbSet<AppUser> AppUsers { get; set; }
        public DbSet<PipelineRunLog> PipelineRunLogs { get; set; }
        public DbSet<UserConnectionPermission> UserConnectionPermissions { get; set; }
        public DbSet<SmtpSettings> SmtpSettings { get; set; }
        public DbSet<ApiKey> ApiKeys { get; set; }
        public DbSet<Notification> Notifications { get; set; }
        public DbSet<SystemSettings> SystemSettings { get; set; }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            base.OnModelCreating(modelBuilder);

            var dictConverter = new Microsoft.EntityFrameworkCore.Storage.ValueConversion.ValueConverter<Dictionary<string, object>, string>(
                v => JsonSerializer.Serialize(v, (JsonSerializerOptions?)null),
                v => JsonSerializer.Deserialize<Dictionary<string, object>>(v, (JsonSerializerOptions?)null) ?? new Dictionary<string, object>()
            );

            var listConverter = new Microsoft.EntityFrameworkCore.Storage.ValueConversion.ValueConverter<List<string>, string>(
                v => JsonSerializer.Serialize(v, (JsonSerializerOptions?)null),
                v => JsonSerializer.Deserialize<List<string>>(v, (JsonSerializerOptions?)null) ?? new List<string>()
            );

            modelBuilder.Entity<DataSourceConfig>()
                .Property(e => e.Parameters)
                .HasConversion(dictConverter);

            modelBuilder.Entity<FieldMap>()
                .Property(e => e.Metadata)
                .HasConversion(dictConverter);

            modelBuilder.Entity<AppUser>()
                .HasIndex(u => u.Username).IsUnique();

            modelBuilder.Entity<UserConnectionPermission>()
                .HasKey(p => new { p.UserId, p.ConnectionProfileId });

            modelBuilder.Entity<PipelineRunLog>()
                .HasOne(r => r.Pipeline)
                .WithMany()
                .HasForeignKey(r => r.PipelineId)
                .OnDelete(DeleteBehavior.Cascade);

            modelBuilder.Entity<PipelineRunLog>()
                .HasIndex(r => r.PipelineId);

            modelBuilder.Entity<PipelineRunLog>()
                .HasIndex(r => r.SnapshotExpiresAt);

            modelBuilder.Entity<ApiKey>(entity =>
            {
                entity.HasIndex(k => k.KeyHash).IsUnique();
                entity.HasOne(k => k.AppUser)
                      .WithMany()
                      .HasForeignKey(k => k.AppUserId)
                      .OnDelete(DeleteBehavior.Cascade);
            });

            modelBuilder.Entity<Notification>(entity =>
            {
                entity.HasIndex(n => n.CreatedAt);
                entity.HasIndex(n => n.IsRead);
                entity.HasOne(n => n.Pipeline)
                      .WithMany()
                      .HasForeignKey(n => n.PipelineId)
                      .OnDelete(DeleteBehavior.SetNull);
            });

            modelBuilder.Entity<Pipeline>(entity =>
            {
                entity.HasOne(p => p.Source)
                      .WithMany()
                      .HasForeignKey("SourceId")
                      .OnDelete(DeleteBehavior.Restrict);

                entity.HasOne(p => p.Destination)
                      .WithMany()
                      .HasForeignKey("DestinationId")
                      .OnDelete(DeleteBehavior.Restrict);

                entity.HasMany(p => p.FieldMappings)
                      .WithOne()
                      .OnDelete(DeleteBehavior.Cascade);

                entity.Property(e => e.Metadata)
                      .HasConversion(dictConverter);

                entity.Property(e => e.Transformations)
                    .HasConversion(listConverter);
            });
        }
    }
}
