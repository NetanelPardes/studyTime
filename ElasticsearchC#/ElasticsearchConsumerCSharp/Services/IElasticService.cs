using ElasticsearchConsumerCSharp.Models;

namespace ElasticsearchConsumerCSharp.Services;

public interface IElasticService
{
    Task SaveAsync(EventMessage message);
}
