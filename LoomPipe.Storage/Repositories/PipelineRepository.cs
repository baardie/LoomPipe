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
            return await _context.Pipelines
                .Include(p => p.Source)
                .Include(p => p.Destination)
                .Include(p => p.FieldMappings)
                .ToListAsync();
        }

        public async Task<Pipeline?> GetByIdAsync(int id)
        {
            return await _context.Pipelines
                .Include(p => p.Source)
                .Include(p => p.Destination)
                .Include(p => p.FieldMappings)
                .FirstOrDefaultAsync(p => p.Id == id);
        }

        public async Task AddAsync(Pipeline pipeline)
        {
            await _context.Pipelines.AddAsync(pipeline);
            await _context.SaveChangesAsync();
        }

        public async Task UpdateAsync(Pipeline pipeline)
        {
            var existing = await _context.Pipelines
                .Include(p => p.Source)
                .Include(p => p.Destination)
                .Include(p => p.FieldMappings)
                .FirstOrDefaultAsync(p => p.Id == pipeline.Id);

            if (existing == null) return;

            // Scalar fields
            existing.Name                    = pipeline.Name;
            existing.ScheduleEnabled         = pipeline.ScheduleEnabled;
            existing.ScheduleIntervalMinutes = pipeline.ScheduleIntervalMinutes;
            existing.NextRunAt               = pipeline.NextRunAt;
            existing.BatchSize               = pipeline.BatchSize;
            existing.BatchDelaySeconds       = pipeline.BatchDelaySeconds;
            existing.Transformations         = pipeline.Transformations;
            existing.Metadata                = pipeline.Metadata;

            // Source — copy into the already-tracked entity
            existing.Source.Name             = pipeline.Source.Name;
            existing.Source.Type             = pipeline.Source.Type;
            existing.Source.ConnectionString = pipeline.Source.ConnectionString;
            existing.Source.Parameters       = pipeline.Source.Parameters;
            existing.Source.Schema           = pipeline.Source.Schema;

            // Destination — copy into the already-tracked entity
            existing.Destination.Name             = pipeline.Destination.Name;
            existing.Destination.Type             = pipeline.Destination.Type;
            existing.Destination.ConnectionString = pipeline.Destination.ConnectionString;
            existing.Destination.Parameters       = pipeline.Destination.Parameters;
            existing.Destination.Schema           = pipeline.Destination.Schema;

            // FieldMappings — replace the collection entirely
            existing.FieldMappings.Clear();
            foreach (var fm in pipeline.FieldMappings)
            {
                existing.FieldMappings.Add(new FieldMap
                {
                    SourceField      = fm.SourceField,
                    DestinationField = fm.DestinationField,
                    AutomapScore     = fm.AutomapScore,
                    IsAutomapped     = fm.IsAutomapped,
                    Metadata         = fm.Metadata,
                });
            }

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
