namespace LoomPipe.Core.Entities
{
    /// <summary>
    /// Singleton settings row (Id always = 1) for system-wide behaviour.
    /// </summary>
    public class SystemSettings
    {
        public int Id { get; set; } = 1;

        /// <summary>
        /// How many days to retain the pipeline configuration snapshot on failed run logs.
        /// After this window elapses the snapshot is cleared and a retry will use the
        /// current live pipeline configuration instead.
        /// </summary>
        public int FailedRunRetentionDays { get; set; } = 7;
    }
}
