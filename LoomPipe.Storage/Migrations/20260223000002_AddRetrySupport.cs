using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace LoomPipe.Storage.Migrations
{
    /// <inheritdoc />
    public partial class AddRetrySupport : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            // ── Extend PipelineRunLogs ───────────────────────────────────────
            migrationBuilder.AddColumn<string>(
                name:     "ConfigSnapshot",
                table:    "PipelineRunLogs",
                type:     "TEXT",
                nullable: true);

            migrationBuilder.AddColumn<DateTime>(
                name:     "SnapshotExpiresAt",
                table:    "PipelineRunLogs",
                type:     "TEXT",
                nullable: true);

            migrationBuilder.AddColumn<int>(
                name:     "RetryOfRunId",
                table:    "PipelineRunLogs",
                type:     "INTEGER",
                nullable: true);

            migrationBuilder.CreateIndex(
                name:   "IX_PipelineRunLogs_SnapshotExpiresAt",
                table:  "PipelineRunLogs",
                column: "SnapshotExpiresAt");

            // ── SystemSettings table ─────────────────────────────────────────
            migrationBuilder.CreateTable(
                name: "SystemSettings",
                columns: table => new
                {
                    Id                     = table.Column<int>(type: "INTEGER", nullable: false)
                                                   .Annotation("Sqlite:Autoincrement", true),
                    FailedRunRetentionDays = table.Column<int>(type: "INTEGER", nullable: false),
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_SystemSettings", x => x.Id);
                });
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(name: "SystemSettings");

            migrationBuilder.DropIndex(
                name:  "IX_PipelineRunLogs_SnapshotExpiresAt",
                table: "PipelineRunLogs");

            migrationBuilder.DropColumn(name: "ConfigSnapshot",     table: "PipelineRunLogs");
            migrationBuilder.DropColumn(name: "SnapshotExpiresAt",  table: "PipelineRunLogs");
            migrationBuilder.DropColumn(name: "RetryOfRunId",       table: "PipelineRunLogs");
        }
    }
}
