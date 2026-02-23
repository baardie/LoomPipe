using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace LoomPipe.Storage.Migrations
{
    /// <inheritdoc />
    public partial class AddCronScheduling : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            // SQLite does not support DROP COLUMN; leave ScheduleIntervalMinutes in place.
            // On SQL Server this would drop the old column, but for SQLite we skip the drop.
            migrationBuilder.AddColumn<string>(
                name: "CronExpression",
                table: "Pipelines",
                type: "TEXT",
                nullable: true);
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropColumn(
                name: "CronExpression",
                table: "Pipelines");
        }
    }
}
