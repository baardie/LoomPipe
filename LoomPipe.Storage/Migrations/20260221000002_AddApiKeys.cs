using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace LoomPipe.Storage.Migrations
{
    /// <inheritdoc />
    public partial class AddApiKeys : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "ApiKeys",
                columns: table => new
                {
                    Id         = table.Column<int>(type: "INTEGER", nullable: false)
                                      .Annotation("Sqlite:Autoincrement", true),
                    AppUserId  = table.Column<int>(type: "INTEGER", nullable: false),
                    Name       = table.Column<string>(type: "TEXT", nullable: false),
                    KeyHash    = table.Column<string>(type: "TEXT", nullable: false),
                    IsActive   = table.Column<bool>(type: "INTEGER", nullable: false),
                    CreatedAt  = table.Column<DateTime>(type: "TEXT", nullable: false),
                    LastUsedAt = table.Column<DateTime>(type: "TEXT", nullable: true),
                    ExpiresAt  = table.Column<DateTime>(type: "TEXT", nullable: true),
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_ApiKeys", x => x.Id);
                    table.ForeignKey(
                        name:       "FK_ApiKeys_AppUsers_AppUserId",
                        column:     x => x.AppUserId,
                        principalTable: "AppUsers",
                        principalColumn: "Id",
                        onDelete:   ReferentialAction.Cascade);
                });

            migrationBuilder.CreateIndex(
                name:    "IX_ApiKeys_KeyHash",
                table:   "ApiKeys",
                column:  "KeyHash",
                unique:  true);

            migrationBuilder.CreateIndex(
                name:   "IX_ApiKeys_AppUserId",
                table:  "ApiKeys",
                column: "AppUserId");
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(name: "ApiKeys");
        }
    }
}
