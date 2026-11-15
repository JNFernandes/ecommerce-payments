using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Ecommerce.Payments.Infrastructure.Persistence.Migrations;

/// <inheritdoc />
public partial class AddOrderCancellationMarkers : Migration
{
    /// <inheritdoc />
    protected override void Up(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.CreateTable(
            name: "order_cancellation_markers",
            columns: table => new
            {
                OrderId = table.Column<Guid>(type: "uuid", nullable: false),
                SourceEventId = table.Column<Guid>(type: "uuid", nullable: false),
                CancelledAt = table.Column<DateTimeOffset>(type: "timestamp with time zone", nullable: false),
                RecordedAt = table.Column<DateTimeOffset>(type: "timestamp with time zone", nullable: false),
                Reason = table.Column<string>(type: "text", nullable: true)
            },
            constraints: table =>
            {
                table.PrimaryKey("PK_order_cancellation_markers", x => x.OrderId);
            });
    }

    /// <inheritdoc />
    protected override void Down(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.DropTable(
            name: "order_cancellation_markers");
    }
}
