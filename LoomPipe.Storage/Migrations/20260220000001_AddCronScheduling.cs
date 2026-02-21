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
            migrationBuilder.DropColumn(
                name: "ScheduleIntervalMinutes",
                table: "Pipelines");

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

            migrationBuilder.AddColumn<int>(
                name: "ScheduleIntervalMinutes",
                table: "Pipelines",
                type: "INTEGER",
                nullable: true);
        }
    }
}
