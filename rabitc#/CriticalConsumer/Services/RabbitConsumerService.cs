using System.Text;
using System.Text.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using CriticalConsumer.Models;

namespace CriticalConsumer.Services;

public class RabbitConsumerService : IRabbitConsumerService
{
    private readonly IConnection _connection;
    private readonly IModel _channel;
    private readonly string _queueName;

    public RabbitConsumerService()
    {
        string rabbitHost =
            Environment.GetEnvironmentVariable("RABBITMQ_HOST")
            ?? throw new Exception("RABBITMQ_HOST missing");

        string exchangeName =
            Environment.GetEnvironmentVariable("RABBITMQ_EXCHANGE")
            ?? throw new Exception("RABBITMQ_EXCHANGE missing");

        _queueName =
            Environment.GetEnvironmentVariable("RABBITMQ_QUEUE")
            ?? throw new Exception("RABBITMQ_QUEUE missing");

        string routingKey =
            Environment.GetEnvironmentVariable("RABBITMQ_ROUTING_KEY")
            ?? throw new Exception("RABBITMQ_ROUTING_KEY missing");


        var factory = new ConnectionFactory
        {
            HostName = rabbitHost
        };


        _connection = factory.CreateConnection();

        _channel = _connection.CreateModel();


        _channel.ExchangeDeclare(
            exchange: exchangeName,
            type: ExchangeType.Direct,
            durable: true
        );


        _channel.QueueDeclare(
            queue: _queueName,
            durable: true,
            exclusive: false,
            autoDelete: false
        );


        _channel.QueueBind(
            queue: _queueName,
            exchange: exchangeName,
            routingKey: routingKey
        );


        _channel.BasicQos(
            prefetchSize: 0,
            prefetchCount: 1,
            global: false
        );
    }


    public void Start()
    {
        var consumer =
            new EventingBasicConsumer(_channel);


        consumer.Received += (sender, eventArgs) =>
        {
            try
            {
                byte[] body =
                    eventArgs.Body.ToArray();


                string json =
                    Encoding.UTF8.GetString(body);


                EventMessage? message =
                    JsonSerializer.Deserialize<EventMessage>(
                        json,
                        new JsonSerializerOptions
                        {
                            PropertyNameCaseInsensitive = true
                        }
                    );


                Console.WriteLine();
                Console.WriteLine("CRITICAL RECEIVED");
                Console.WriteLine(
                    $"Id: {message?.Id}"
                );
                Console.WriteLine(
                    $"Server: {message?.Server}"
                );
                Console.WriteLine(
                    $"Type: {message?.Type}"
                );
                Console.WriteLine(
                    $"Value: {message?.Value}"
                );
                Console.WriteLine();


                _channel.BasicAck(
                    deliveryTag: eventArgs.DeliveryTag,
                    multiple: false
                );
            }
            catch (Exception ex)
            {
                Console.WriteLine(
                    $"Error: {ex.Message}"
                );


                _channel.BasicNack(
                    deliveryTag: eventArgs.DeliveryTag,
                    multiple: false,
                    requeue: false
                );
            }
        };


        _channel.BasicConsume(
            queue: _queueName,
            autoAck: false,
            consumer: consumer
        );


        Console.WriteLine(
            "Waiting for critical messages..."
        );


        Thread.Sleep(Timeout.Infinite);
    }
}