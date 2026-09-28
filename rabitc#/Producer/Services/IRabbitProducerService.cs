using Producer.Models;

namespace Producer.Services;

public interface IRabbitProducerService
{
    void SendMessage(EventMessage message);
}