# syntax=docker/dockerfile:1

# ---- build ----
FROM --platform=$BUILDPLATFORM mcr.microsoft.com/dotnet/sdk:10.0 AS build
ARG TARGETARCH
WORKDIR /src

# Shared build settings first, then project files only, so `dotnet restore` is cached
# until a project file or shared props change.
COPY Directory.Build.props BannedSymbols.txt .editorconfig ./
COPY src/Ecommerce.Payments.Domain/Ecommerce.Payments.Domain.csproj src/Ecommerce.Payments.Domain/
COPY src/Ecommerce.Payments.Service/Ecommerce.Payments.Service.csproj src/Ecommerce.Payments.Service/
COPY src/Ecommerce.Payments.Infrastructure/Ecommerce.Payments.Infrastructure.csproj src/Ecommerce.Payments.Infrastructure/
COPY src/Ecommerce.Payments.Consumer/Ecommerce.Payments.Consumer.csproj src/Ecommerce.Payments.Consumer/
# Restore and publish for the target Linux architecture only (amd64 -> x64, arm64 -> arm64), so
# the image carries just that librdkafka native library instead of one per OS/architecture.
# The SDK accepts Docker's TARGETARCH names (amd64, arm64) for --arch.
RUN dotnet restore src/Ecommerce.Payments.Consumer/Ecommerce.Payments.Consumer.csproj \
    --arch $TARGETARCH

COPY src/ src/
RUN dotnet publish src/Ecommerce.Payments.Consumer/Ecommerce.Payments.Consumer.csproj \
    --configuration Release \
    --no-restore \
    --arch $TARGETARCH \
    --self-contained false \
    --output /app/publish \
    /p:UseAppHost=false

# ---- runtime ----
# Chiseled Ubuntu: the smallest glibc-based .NET runtime image (no shell, no package manager,
# non-root by default). glibc is what Confluent.Kafka's bundled librdkafka.so expects.
FROM mcr.microsoft.com/dotnet/runtime:10.0-noble-chiseled AS runtime
WORKDIR /app
COPY --from=build /app/publish .

# All settings come from appsettings.json, overridden by environment variables using `__` as
# the section separator, e.g. ConnectionStrings__Payments, Kafka__BootstrapServers,
# OutboxRelay__PollIntervalMs. Pending EF Core migrations are applied on startup, before any
# consumer runs (DatabaseMigration__*).
ENV DOTNET_ENVIRONMENT=Production

# Non-root user shipped with the official .NET images (UID 1654).
USER $APP_UID

ENTRYPOINT ["dotnet", "Ecommerce.Payments.Consumer.dll"]
