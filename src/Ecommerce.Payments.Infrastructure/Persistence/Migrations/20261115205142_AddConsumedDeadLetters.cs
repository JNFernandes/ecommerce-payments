using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Ecommerce.Payments.Infrastructure.Persistence.Migrations;

/// <inheritdoc />
public partial class AddConsumedDeadLetters : Migration
{
    private static readonly string[] PositionColumns = ["Topic", "Partition", "Offset"];

    /// <inheritdoc />
    protected override void Up(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.CreateTable(
            name: "consumed_dead_letters",
            columns: table => new
            {
                Id = table.Column<Guid>(type: "uuid", nullable: false),
                Topic = table.Column<string>(type: "text", nullable: false),
                Partition = table.Column<int>(type: "integer", nullable: false),
                Offset = table.Column<long>(type: "bigint", nullable: false),
                MessageKey = table.Column<string>(type: "text", nullable: true),
                Payload = table.Column<string>(type: "text", nullable: true),
                Error = table.Column<string>(type: "text", nullable: false),
                Attempts = table.Column<int>(type: "integer", nullable: false),
                FailedAt = table.Column<DateTimeOffset>(type: "timestamp with time zone", nullable: false)
            },
            constraints: table =>
            {
                table.PrimaryKey("PK_consumed_dead_letters", x => x.Id);
                table.CheckConstraint("CK_consumed_dead_letters_attempts", "\"Attempts\" >= 1");
            });

        migrationBuilder.CreateIndex(
            name: "IX_consumed_dead_letters_position",
            table: "consumed_dead_letters",
            columns: PositionColumns,
            unique: true);
    }

    /// <inheritdoc />
    protected override void Down(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.DropTable(
            name: "consumed_dead_letters");
    }
}
