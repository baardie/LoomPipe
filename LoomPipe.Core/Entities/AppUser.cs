using System;

namespace LoomPipe.Core.Entities
{
    public class AppUser
    {
        public int Id { get; set; }
        public string Username { get; set; } = string.Empty;
        public string PasswordHash { get; set; } = string.Empty;
        public string Role { get; set; } = "User"; // Admin | User | Guest
        public DateTime CreatedAt { get; set; }
        public bool IsActive { get; set; } = true;
    }
}
