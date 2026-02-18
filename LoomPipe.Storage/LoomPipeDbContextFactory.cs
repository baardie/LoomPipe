using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;

namespace LoomPipe.Storage
{
    public class LoomPipeDbContextFactory : IDesignTimeDbContextFactory<LoomPipeDbContext>
    {
        public LoomPipeDbContext CreateDbContext(string[] args)
        {
            var optionsBuilder = new DbContextOptionsBuilder<LoomPipeDbContext>();
            optionsBuilder.UseSqlite("Data Source=loompipe.db");
            return new LoomPipeDbContext(optionsBuilder.Options);
        }
    }
}
