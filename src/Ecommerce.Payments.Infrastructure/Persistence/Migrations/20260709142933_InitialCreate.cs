using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Ecommerce.Payments.Infrastructure.Persistence.Migrations;

/// <inheritdoc />
public partial class InitialCreate : Migration
{
    /// <inheritdoc />
    protected override void Up(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.CreateTable(
            name: "payment_dead_letters",
            columns: table => new
            {
                Id = table.Column<Guid>(type: "uuid", nullable: false),
                PaymentId = table.Column<Guid>(type: "uuid", nullable: false),
                EventType = table.Column<string>(type: "text", nullable: false),
                Payload = table.Column<string>(type: "jsonb", nullable: false),
                FailureReason = table.Column<string>(type: "text", nullable: false),
                CreatedAt = table.Column<DateTimeOffset>(type: "timestamp with time zone", nullable: false)
            },
            constraints: table =>
            {
                table.PrimaryKey("PK_payment_dead_letters", x => x.Id);
            });

        migrationBuilder.CreateTable(
            name: "payments",
            columns: table => new
            {
                Id = table.Column<Guid>(type: "uuid", nullable: false),
                OrderId = table.Column<Guid>(type: "uuid", nullable: false),
                CustomerId = table.Column<Guid>(type: "uuid", nullable: false),
                Amount = table.Column<decimal>(type: "numeric", nullable: false),
                Currency = table.Column<string>(type: "character varying(3)", maxLength: 3, nullable: false),
                Status = table.Column<string>(type: "character varying(20)", maxLength: 20, nullable: false),
                SourceEventId = table.Column<Guid>(type: "uuid", nullable: false),
                CreatedAt = table.Column<DateTimeOffset>(type: "timestamp with time zone", nullable: false),
                ProcessedAt = table.Column<DateTimeOffset>(type: "timestamp with time zone", nullable: true)
            },
            constraints: table =>
            {
                table.PrimaryKey("PK_payments", x => x.Id);
            });

        migrationBuilder.CreateIndex(
            name: "IX_payments_OrderId",
            table: "payments",
            column: "OrderId",
            unique: true);
    }

    /// <inheritdoc />
    protected override void Down(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.DropTable(
            name: "payment_dead_letters");

        migrationBuilder.DropTable(
            name: "payments");
    }
}
