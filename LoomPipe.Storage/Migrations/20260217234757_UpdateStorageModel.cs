using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace LoomPipe.Storage.Migrations
{
    /// <inheritdoc />
    public partial class UpdateStorageModel : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropForeignKey(
                name: "FK_FieldMap_Pipelines_PipelineId",
                table: "FieldMap");

            migrationBuilder.DropForeignKey(
                name: "FK_Pipelines_DataSourceConfigs_DestinationId",
                table: "Pipelines");

            migrationBuilder.DropForeignKey(
                name: "FK_Pipelines_DataSourceConfigs_SourceId",
                table: "Pipelines");

            migrationBuilder.DropPrimaryKey(
                name: "PK_FieldMap",
                table: "FieldMap");

            migrationBuilder.RenameTable(
                name: "FieldMap",
                newName: "FieldMaps");

            migrationBuilder.RenameIndex(
                name: "IX_FieldMap_PipelineId",
                table: "FieldMaps",
                newName: "IX_FieldMaps_PipelineId");

            migrationBuilder.AddPrimaryKey(
                name: "PK_FieldMaps",
                table: "FieldMaps",
                column: "Id");

            migrationBuilder.AddForeignKey(
                name: "FK_FieldMaps_Pipelines_PipelineId",
                table: "FieldMaps",
                column: "PipelineId",
                principalTable: "Pipelines",
                principalColumn: "Id",
                onDelete: ReferentialAction.Cascade);

            migrationBuilder.AddForeignKey(
                name: "FK_Pipelines_DataSourceConfigs_DestinationId",
                table: "Pipelines",
                column: "DestinationId",
                principalTable: "DataSourceConfigs",
                principalColumn: "Id",
                onDelete: ReferentialAction.Restrict);

            migrationBuilder.AddForeignKey(
                name: "FK_Pipelines_DataSourceConfigs_SourceId",
                table: "Pipelines",
                column: "SourceId",
                principalTable: "DataSourceConfigs",
                principalColumn: "Id",
                onDelete: ReferentialAction.Restrict);
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropForeignKey(
                name: "FK_FieldMaps_Pipelines_PipelineId",
                table: "FieldMaps");

            migrationBuilder.DropForeignKey(
                name: "FK_Pipelines_DataSourceConfigs_DestinationId",
                table: "Pipelines");

            migrationBuilder.DropForeignKey(
                name: "FK_Pipelines_DataSourceConfigs_SourceId",
                table: "Pipelines");

            migrationBuilder.DropPrimaryKey(
                name: "PK_FieldMaps",
                table: "FieldMaps");

            migrationBuilder.RenameTable(
                name: "FieldMaps",
                newName: "FieldMap");

            migrationBuilder.RenameIndex(
                name: "IX_FieldMaps_PipelineId",
                table: "FieldMap",
                newName: "IX_FieldMap_PipelineId");

            migrationBuilder.AddPrimaryKey(
                name: "PK_FieldMap",
                table: "FieldMap",
                column: "Id");

            migrationBuilder.AddForeignKey(
                name: "FK_FieldMap_Pipelines_PipelineId",
                table: "FieldMap",
                column: "PipelineId",
                principalTable: "Pipelines",
                principalColumn: "Id");

            migrationBuilder.AddForeignKey(
                name: "FK_Pipelines_DataSourceConfigs_DestinationId",
                table: "Pipelines",
                column: "DestinationId",
                principalTable: "DataSourceConfigs",
                principalColumn: "Id",
                onDelete: ReferentialAction.Cascade);

            migrationBuilder.AddForeignKey(
                name: "FK_Pipelines_DataSourceConfigs_SourceId",
                table: "Pipelines",
                column: "SourceId",
                principalTable: "DataSourceConfigs",
                principalColumn: "Id",
                onDelete: ReferentialAction.Cascade);
        }
    }
}
