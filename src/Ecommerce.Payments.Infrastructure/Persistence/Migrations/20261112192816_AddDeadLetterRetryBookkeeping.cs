using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Ecommerce.Payments.Infrastructure.Persistence.Migrations;

/// <inheritdoc />
public partial class AddDeadLetterRetryBookkeeping : Migration
{
    /// <inheritdoc />
    protected override void Up(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.AddColumn<DateTimeOffset>(
            name: "LastAttemptAt",
            table: "payment_dead_letters",
            type: "timestamp with time zone",
            nullable: true);

        migrationBuilder.AddColumn<string>(
            name: "LastError",
            table: "payment_dead_letters",
            type: "text",
            nullable: true);

        migrationBuilder.AddColumn<string>(
            name: "MessageKey",
            table: "payment_dead_letters",
            type: "text",
            nullable: false,
            defaultValue: "");

        migrationBuilder.AddColumn<DateTimeOffset>(
            name: "NextRetryAt",
            table: "payment_dead_letters",
            type: "timestamp with time zone",
            nullable: true);

        migrationBuilder.AddColumn<DateTimeOffset>(
            name: "ResolvedAt",
            table: "payment_dead_letters",
            type: "timestamp with time zone",
            nullable: true);

        migrationBuilder.AddColumn<int>(
            name: "RetryCount",
            table: "payment_dead_letters",
            type: "integer",
            nullable: false,
            defaultValue: 0);

        migrationBuilder.AddColumn<string>(
            name: "Status",
            table: "payment_dead_letters",
            type: "character varying(20)",
            maxLength: 20,
            nullable: false,
            defaultValue: "");

        migrationBuilder.AddColumn<string>(
            name: "Topic",
            table: "payment_dead_letters",
            type: "text",
            nullable: false,
            defaultValue: "");

        // Backfill rows written before US-03: make them due immediately and recover the topic
        // and key the publisher used (key = payment id; topic from the event type). An unknown
        // event type gets an empty topic, so its publish fails and the row ends Exhausted
        // instead of being silently dropped (spec FR-002, data-model.md).
        migrationBuilder.Sql(
            """
            UPDATE payment_dead_letters
            SET "Status" = 'PendingRetry',
                "RetryCount" = 0,
                "NextRetryAt" = now(),
                "MessageKey" = "PaymentId"::text,
                "Topic" = CASE "EventType"
                    WHEN 'PaymentProcessedEvent' THEN 'payments.payment-processed'
                    WHEN 'PaymentFailedEvent' THEN 'payments.payment-failed'
                    ELSE ''
                END;
            """);

        migrationBuilder.CreateIndex(
            name: "IX_payment_dead_letters_due",
            table: "payment_dead_letters",
            column: "NextRetryAt",
            filter: "\"Status\" = 'PendingRetry'");
    }

    /// <inheritdoc />
    protected override void Down(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.DropIndex(
            name: "IX_payment_dead_letters_due",
            table: "payment_dead_letters");

        migrationBuilder.DropColumn(
            name: "LastAttemptAt",
            table: "payment_dead_letters");

        migrationBuilder.DropColumn(
            name: "LastError",
            table: "payment_dead_letters");

        migrationBuilder.DropColumn(
            name: "MessageKey",
            table: "payment_dead_letters");

        migrationBuilder.DropColumn(
            name: "NextRetryAt",
            table: "payment_dead_letters");

        migrationBuilder.DropColumn(
            name: "ResolvedAt",
            table: "payment_dead_letters");

        migrationBuilder.DropColumn(
            name: "RetryCount",
            table: "payment_dead_letters");

        migrationBuilder.DropColumn(
            name: "Status",
            table: "payment_dead_letters");

        migrationBuilder.DropColumn(
            name: "Topic",
            table: "payment_dead_letters");
    }
}
