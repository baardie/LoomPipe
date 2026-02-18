using LoomPipe.Core.DTOs;
using LoomPipe.Core.Entities;

namespace LoomPipe.Core.Interfaces
{
    public interface IConnectionProfileService
    {
        Task<IEnumerable<ConnectionProfileSummary>> GetAllAsync();
        Task<ConnectionProfileSummary?> GetSummaryAsync(int id);
        Task<ConnectionProfileSummary> CreateAsync(CreateConnectionProfileDto dto);
        Task UpdateAsync(int id, UpdateConnectionProfileDto dto);
        Task DeleteAsync(int id);

        /// <summary>
        /// Decrypts credentials and builds the provider-specific connection string.
        /// The result is used at runtime only — never persisted.
        /// </summary>
        Task<string> BuildConnectionStringAsync(int profileId);

        Task<ConnectionTestResult> TestConnectionAsync(int id);
        Task RecordTestResultAsync(int id, bool success);
    }
}
