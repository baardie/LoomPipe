using LoomPipe.Core.Entities;
using System.Collections.Generic;
using System.Dynamic;
using System.Threading.Tasks;

namespace LoomPipe.Core.Interfaces
{
    /// <summary>
    /// Defines the contract for a source reader connector. 
    /// All I/O must be async.
    /// </summary>
    public interface ISourceReader
    {
        /// <summary>
        /// Asynchronously reads all data from the source.
        /// </summary>
        /// <param name="config">The data source configuration.</param>
        /// <returns>A collection of data records, typically as dynamic objects.</returns>
        Task<IEnumerable<object>> ReadAsync(DataSourceConfig config);

        /// <summary>
        /// Asynchronously discovers the schema of the source.
        /// </summary>
        /// <param name="config">The data source configuration.</param>
        /// <returns>A collection of field names representing the schema.</returns>
        Task<IEnumerable<string>> DiscoverSchemaAsync(DataSourceConfig config);

        /// <summary>
        /// Asynchronously reads a sample of data from the source for a dry run.
        /// </summary>
        /// <param name="config">The data source configuration.</param>
        /// <param name="sampleSize">The number of records to read.</param>
        /// <returns>A collection of sample data records.</returns>
        Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, int sampleSize = 10);
    }
}
