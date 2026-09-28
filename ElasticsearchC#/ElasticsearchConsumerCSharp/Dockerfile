FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build

WORKDIR /src

COPY ElasticsearchConsumerCSharp.csproj .

RUN dotnet restore

COPY . .

RUN dotnet publish -c Release -o /app/publish


FROM mcr.microsoft.com/dotnet/runtime:8.0

WORKDIR /app

COPY --from=build /app/publish .

CMD ["dotnet", "ElasticsearchConsumerCSharp.dll"]