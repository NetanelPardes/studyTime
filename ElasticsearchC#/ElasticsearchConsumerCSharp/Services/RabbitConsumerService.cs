using System.Text;
using System.Text.Json;

using ElasticsearchConsumerCSharp.Models;

using RabbitMQ.Client;
using RabbitMQ.Client.Events;


namespace ElasticsearchConsumerCSharp.Services;

public class RabbitConsumerService
    : IRabbitConsumerService
{
    private readonly IElasticService _elasticService;

    private readonly string _rabbitHost;
    private readonly int _rabbitPort;

    private readonly string _rabbitExchange;
    private readonly string _rabbitQueue;
    private readonly string _rabbitRoutingKey;


    public RabbitConsumerService(
        IElasticService elasticService
    )
    {
        _elasticService =
            elasticService;


        _rabbitHost =
            Environment.GetEnvironmentVariable(
                "RABBITMQ_HOST"
            )
            ?? throw new Exception(
                "RABBITMQ_HOST missing"
            );


        _rabbitPort =
            int.Parse(
                Environment.GetEnvironmentVariable(
                    "RABBITMQ_PORT"
                )
                ?? "5672"
            );


        _rabbitExchange =
            Environment.GetEnvironmentVariable(
                "RABBITMQ_EXCHANGE"
            )
            ?? throw new Exception(
                "RABBITMQ_EXCHANGE missing"
            );


        _rabbitQueue =
            Environment.GetEnvironmentVariable(
                "RABBITMQ_QUEUE"
            )
            ?? throw new Exception(
                "RABBITMQ_QUEUE missing"
            );


        _rabbitRoutingKey =
            Environment.GetEnvironmentVariable(
                "RABBITMQ_ROUTING_KEY"
            )
            ?? throw new Exception(
                "RABBITMQ_ROUTING_KEY missing"
            );
    }


    public void Start()
    {
        ConnectionFactory factory = new()
        {
            HostName = _rabbitHost,
            Port = _rabbitPort
        };


        IConnection? connection = null;


        while (connection == null)
        {
            try
            {
                Console.WriteLine(
                    $"Connecting to RabbitMQ: " +
                    $"{_rabbitHost}:{_rabbitPort}"
                );


                connection =
                    factory.CreateConnection();


                Console.WriteLine(
                    "Connected to RabbitMQ"
                );
            }
            catch
            {
                Console.WriteLine(
                    "RabbitMQ is not ready. " +
                    "Retrying in 5 seconds..."
                );


                Thread.Sleep(5000);
            }
        }


        IModel channel =
            connection.CreateModel();


        channel.ExchangeDeclare(
            exchange: _rabbitExchange,
            type: ExchangeType.Direct,
            durable: true
        );


        channel.QueueDeclare(
            queue: _rabbitQueue,
            durable: true,
            exclusive: false,
            autoDelete: false
        );


        channel.QueueBind(
            queue: _rabbitQueue,
            exchange: _rabbitExchange,
            routingKey: _rabbitRoutingKey
        );


        channel.BasicQos(
            prefetchSize: 0,
            prefetchCount: 1,
            global: false
        );


        EventingBasicConsumer consumer =
            new(channel);


        consumer.Received += async (
            sender,
            eventArgs
        ) =>
        {
            try
            {
                byte[] body =
                    eventArgs.Body.ToArray();


                string json =
                    Encoding.UTF8.GetString(
                        body
                    );


                EventMessage? message =
                    JsonSerializer.Deserialize<EventMessage>(
                        json,
                        new JsonSerializerOptions
                        {
                            PropertyNameCaseInsensitive =
                                true
                        }
                    );


                if (message == null)
                {
                    throw new Exception(
                        "Message is null"
                    );
                }


                Console.WriteLine(
                    $"RabbitMQ received: {message.Id}"
                );


                await _elasticService.SaveAsync(
                    message
                );


                channel.BasicAck(
                    deliveryTag:
                        eventArgs.DeliveryTag,

                    multiple:
                        false
                );
            }
            catch (Exception error)
            {
                Console.WriteLine(
                    $"Error: {error.Message}"
                );


                channel.BasicNack(
                    deliveryTag:
                        eventArgs.DeliveryTag,

                    multiple:
                        false,

                    requeue:
                        false
                );
            }
        };


        channel.BasicConsume(
            queue: _rabbitQueue,
            autoAck: false,
            consumer: consumer
        );


        Console.WriteLine(
            "Waiting for RabbitMQ messages..."
        );


        Thread.Sleep(
            Timeout.Infinite
        );
    }
}