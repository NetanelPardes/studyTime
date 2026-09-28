using System.Text;
using System.Text.Json;
using Producer.Models;
using RabbitMQ.Client;

namespace Producer.Services;

public class RabbitProducerService : IRabbitProducerService
{
    private readonly IConnection _connection;
    private readonly IModel _channel;
    private readonly string _exchangeName;

    public RabbitProducerService()
    {
        string rabbitHost =
            Environment.GetEnvironmentVariable("RABBITMQ_HOST")
            ?? throw new Exception("RABBITMQ_HOST missing");

        _exchangeName =
            Environment.GetEnvironmentVariable("RABBITMQ_EXCHANGE")
            ?? throw new Exception("RABBITMQ_EXCHANGE missing");


        var factory = new ConnectionFactory
        {
            HostName = rabbitHost
        };


        _connection = factory.CreateConnection();

        _channel = _connection.CreateModel();


        _channel.ExchangeDeclare(
            exchange: _exchangeName,
            type: ExchangeType.Direct,
            durable: true
        );


        _channel.QueueDeclare(
            queue: "warning-queue",
            durable: true,
            exclusive: false,
            autoDelete: false
        );


        _channel.QueueDeclare(
            queue: "critical-queue",
            durable: true,
            exclusive: false,
            autoDelete: false
        );


        _channel.QueueBind(
            queue: "warning-queue",
            exchange: _exchangeName,
            routingKey: "alert.warning"
        );


        _channel.QueueBind(
            queue: "critical-queue",
            exchange: _exchangeName,
            routingKey: "alert.critical"
        );
    }


    public void SendMessage(EventMessage message)
    {
        string routingKey;


        if (message.Value >= 90)
        {
            routingKey = "alert.critical";
            message.Status = "Critical";
        }
        else
        {
            routingKey = "alert.warning";
            message.Status = "Warning";
        }


        string json =
            JsonSerializer.Serialize(message);


        byte[] body =
            Encoding.UTF8.GetBytes(json);


        var properties =
            _channel.CreateBasicProperties();


        properties.Persistent = true;


        _channel.BasicPublish(
            exchange: _exchangeName,
            routingKey: routingKey,
            basicProperties: properties,
            body: body
        );


        Console.WriteLine(
            $"Sent {message.Status}: {message.Id} - {message.Value}"
        );
    }
}