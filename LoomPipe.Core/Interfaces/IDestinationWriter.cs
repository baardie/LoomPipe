using LoomPipe.Core.Entities;
using System.Collections.Generic;
using System.Dynamic;
using System.Threading.Tasks;

namespace LoomPipe.Core.Interfaces
{
    /// <summary>
    /// Interface for writing data to a destination. All I/O must be async.
    /// </summary>
    public interface IDestinationWriter
    {
        Task WriteAsync(DataSourceConfig config, IEnumerable<object> records);
        Task<bool> ValidateSchemaAsync(DataSourceConfig config, IEnumerable<string> fields);
        Task<IEnumerable<object>> DryRunPreviewAsync(DataSourceConfig config, IEnumerable<object> records, int sampleSize = 10);
    }
}
