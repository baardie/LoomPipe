using LoomPipe.Core.Entities;
using Microsoft.EntityFrameworkCore;

namespace LoomPipe.Data.Database
{
    public class LoomPipeDbContext : DbContext
    {
        public LoomPipeDbContext(DbContextOptions<LoomPipeDbContext> options) : base(options)
        {
        }

        public DbSet<Connector> Connectors { get; set; }
        public DbSet<ConnectorParameter> ConnectorParameters { get; set; }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            base.OnModelCreating(modelBuilder);
        }
    }
}
