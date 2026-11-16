using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Ecommerce.Payments.Infrastructure.Persistence.Migrations;

/// <inheritdoc />
public partial class AddRetentionIndexes : Migration
{
    /// <inheritdoc />
    protected override void Up(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.CreateIndex(
            name: "IX_payment_outbox_published_at",
            table: "payment_outbox",
            column: "PublishedAt",
            filter: "\"Status\" = 'Published'");

        migrationBuilder.CreateIndex(
            name: "IX_order_cancellation_markers_recorded_at",
            table: "order_cancellation_markers",
            column: "RecordedAt");

        migrationBuilder.CreateIndex(
            name: "IX_consumed_dead_letters_failed_at",
            table: "consumed_dead_letters",
            column: "FailedAt");
    }

    /// <inheritdoc />
    protected override void Down(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.DropIndex(
            name: "IX_payment_outbox_published_at",
            table: "payment_outbox");

        migrationBuilder.DropIndex(
            name: "IX_order_cancellation_markers_recorded_at",
            table: "order_cancellation_markers");

        migrationBuilder.DropIndex(
            name: "IX_consumed_dead_letters_failed_at",
            table: "consumed_dead_letters");
    }
}
