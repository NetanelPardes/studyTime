FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build

WORKDIR /src

COPY Producer.csproj .

RUN dotnet restore

COPY . .

RUN dotnet publish -c Release -o /app/publish


FROM mcr.microsoft.com/dotnet/runtime:8.0

WORKDIR /app

COPY --from=build /app/publish .

COPY events.json ./events.json
COPY events.csv ./events.csv

CMD ["dotnet", "Producer.dll"]