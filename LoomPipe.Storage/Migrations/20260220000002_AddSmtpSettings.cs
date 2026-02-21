using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace LoomPipe.Storage.Migrations
{
    /// <inheritdoc />
    public partial class AddSmtpSettings : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "SmtpSettings",
                columns: table => new
                {
                    Id                = table.Column<int>(type: "INTEGER", nullable: false),
                    Enabled           = table.Column<bool>(type: "INTEGER", nullable: false),
                    SmtpHost          = table.Column<string>(type: "TEXT", nullable: false),
                    SmtpPort          = table.Column<int>(type: "INTEGER", nullable: false),
                    EnableSsl         = table.Column<bool>(type: "INTEGER", nullable: false),
                    Username          = table.Column<string>(type: "TEXT", nullable: false),
                    EncryptedPassword = table.Column<string>(type: "TEXT", nullable: false),
                    FromAddress       = table.Column<string>(type: "TEXT", nullable: false),
                    FromName          = table.Column<string>(type: "TEXT", nullable: false),
                    AdminEmail        = table.Column<string>(type: "TEXT", nullable: false),
                    NotifyOnFailure   = table.Column<bool>(type: "INTEGER", nullable: false),
                    NotifyOnSuccess   = table.Column<bool>(type: "INTEGER", nullable: false),
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_SmtpSettings", x => x.Id);
                });
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(name: "SmtpSettings");
        }
    }
}
