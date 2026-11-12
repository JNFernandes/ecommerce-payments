# syntax=docker/dockerfile:1

# ---- build ----
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

# Shared build settings first, then project files only, so `dotnet restore` is cached
# until a project file or shared props change.
COPY Directory.Build.props BannedSymbols.txt .editorconfig ./
COPY src/Ecommerce.Payments.Domain/Ecommerce.Payments.Domain.csproj src/Ecommerce.Payments.Domain/
COPY src/Ecommerce.Payments.Service/Ecommerce.Payments.Service.csproj src/Ecommerce.Payments.Service/
COPY src/Ecommerce.Payments.Infrastructure/Ecommerce.Payments.Infrastructure.csproj src/Ecommerce.Payments.Infrastructure/
COPY src/Ecommerce.Payments.Consumer/Ecommerce.Payments.Consumer.csproj src/Ecommerce.Payments.Consumer/
RUN dotnet restore src/Ecommerce.Payments.Consumer/Ecommerce.Payments.Consumer.csproj

COPY src/ src/
RUN dotnet publish src/Ecommerce.Payments.Consumer/Ecommerce.Payments.Consumer.csproj \
    --configuration Release \
    --no-restore \
    --output /app/publish \
    /p:UseAppHost=false

# ---- runtime ----
FROM mcr.microsoft.com/dotnet/runtime:10.0 AS runtime
WORKDIR /app
COPY --from=build /app/publish .

# Non-root user shipped with the official .NET images.
USER $APP_UID

ENTRYPOINT ["dotnet", "Ecommerce.Payments.Consumer.dll"]
