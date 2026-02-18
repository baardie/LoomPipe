namespace LoomPipe.Core.Entities
{
    /// <summary>
    /// Join entity granting a specific user access to a specific connection profile.
    /// Admins always have full access; this table only governs User-role access.
    /// </summary>
    public class UserConnectionPermission
    {
        public int UserId { get; set; }
        public int ConnectionProfileId { get; set; }
    }
}
