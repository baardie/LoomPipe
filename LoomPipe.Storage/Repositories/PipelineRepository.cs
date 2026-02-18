using LoomPipe.Core.Entities;
using LoomPipe.Storage.Interfaces;
using Microsoft.EntityFrameworkCore;

namespace LoomPipe.Storage.Repositories
{
    public class PipelineRepository : IPipelineRepository
    {
        private readonly LoomPipeDbContext _context;
        public PipelineRepository(LoomPipeDbContext context)
        {
            _context = context;
        }

        public async Task<IEnumerable<Pipeline>> GetAllAsync()
        {
            return await _context.Pipelines.ToListAsync();
        }

        public async Task<Pipeline?> GetByIdAsync(int id)
        {
            return await _context.Pipelines.FindAsync(id);
        }

        public async Task AddAsync(Pipeline pipeline)
        {
            await _context.Pipelines.AddAsync(pipeline);
            await _context.SaveChangesAsync();
        }

        public async Task UpdateAsync(Pipeline pipeline)
        {
            _context.Pipelines.Update(pipeline);
            await _context.SaveChangesAsync();
        }

        public async Task DeleteAsync(int id)
        {
            var pipeline = await _context.Pipelines.FindAsync(id);
            if (pipeline != null)
            {
                _context.Pipelines.Remove(pipeline);
                await _context.SaveChangesAsync();
            }
        }
    }
}
