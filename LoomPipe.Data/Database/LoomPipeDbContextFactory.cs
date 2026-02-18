using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;
using Microsoft.Extensions.Configuration;
using System.IO;

namespace LoomPipe.Data.Database
{
    public class LoomPipeDbContextFactory : IDesignTimeDbContextFactory<LoomPipeDbContext>
    {
        public LoomPipeDbContext CreateDbContext(string[] args)
        {
            var serverProjectDirectory = Path.GetFullPath(Path.Combine(Directory.GetCurrentDirectory(), "..", "LoomPipe.Server"));
            
            IConfigurationRoot configuration = new ConfigurationBuilder()
                .SetBasePath(serverProjectDirectory)
                .AddJsonFile("appsettings.json")
                .Build();

            var builder = new DbContextOptionsBuilder<LoomPipeDbContext>();
            var connectionString = configuration.GetConnectionString("DefaultConnection");
            builder.UseSqlServer(connectionString);

            return new LoomPipeDbContext(builder.Options);
        }
    }
}
