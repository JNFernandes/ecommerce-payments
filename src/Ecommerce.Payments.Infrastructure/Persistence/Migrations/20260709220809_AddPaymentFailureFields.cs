using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Ecommerce.Payments.Infrastructure.Persistence.Migrations;

/// <inheritdoc />
public partial class AddPaymentFailureFields : Migration
{
    /// <inheritdoc />
    protected override void Up(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.AddColumn<DateTimeOffset>(
            name: "FailedAt",
            table: "payments",
            type: "timestamp with time zone",
            nullable: true);

        migrationBuilder.AddColumn<string>(
            name: "FailureReason",
            table: "payments",
            type: "text",
            nullable: true);
    }

    /// <inheritdoc />
    protected override void Down(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.DropColumn(
            name: "FailedAt",
            table: "payments");

        migrationBuilder.DropColumn(
            name: "FailureReason",
            table: "payments");
    }
}
