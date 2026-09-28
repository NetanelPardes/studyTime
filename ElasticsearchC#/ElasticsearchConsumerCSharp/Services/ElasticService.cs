using Elastic.Clients.Elasticsearch;
using ElasticsearchConsumerCSharp.Models;

namespace ElasticsearchConsumerCSharp.Services;

public class ElasticService : IElasticService
{
    private readonly ElasticsearchClient _client;
    private readonly string _indexName;


    public ElasticService()
    {
        string elasticUrl =
            Environment.GetEnvironmentVariable(
                "ELASTICSEARCH_URL"
            )
            ?? throw new Exception(
                "ELASTICSEARCH_URL missing"
            );


        _indexName =
            Environment.GetEnvironmentVariable(
                "ELASTICSEARCH_INDEX"
            )
            ?? throw new Exception(
                "ELASTICSEARCH_INDEX missing"
            );


        ElasticsearchClientSettings settings =
            new ElasticsearchClientSettings(
                new Uri(elasticUrl)
            );


        _client =
            new ElasticsearchClient(
                settings
            );
    }


    public async Task SaveAsync(
        EventMessage message
    )
    {
        IndexResponse response =
            await _client.IndexAsync(
                message,
                i => i
                    .Index(_indexName)
                    .Id(message.Id)
            );


        if (!response.IsValidResponse)
        {
            throw new Exception(
                "Failed to save to Elasticsearch"
            );
        }


        Console.WriteLine(
            $"Saved to Elasticsearch: {message.Id}"
        );
    }
}