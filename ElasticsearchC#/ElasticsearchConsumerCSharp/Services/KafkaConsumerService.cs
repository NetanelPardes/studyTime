using System.Text.Json;

using Confluent.Kafka;

using ElasticsearchConsumerCSharp.Models;


namespace ElasticsearchConsumerCSharp.Services;

public class KafkaConsumerService
    : IKafkaConsumerService
{
    private readonly IElasticService _elasticService;

    private readonly string _bootstrapServers;
    private readonly string _topic;
    private readonly string _groupId;


    public KafkaConsumerService(
        IElasticService elasticService
    )
    {
        _elasticService =
            elasticService;


        _bootstrapServers =
            Environment.GetEnvironmentVariable(
                "KAFKA_BOOTSTRAP_SERVERS"
            )
            ?? throw new Exception(
                "KAFKA_BOOTSTRAP_SERVERS missing"
            );


        _topic =
            Environment.GetEnvironmentVariable(
                "KAFKA_TOPIC"
            )
            ?? throw new Exception(
                "KAFKA_TOPIC missing"
            );


        _groupId =
            Environment.GetEnvironmentVariable(
                "KAFKA_GROUP_ID"
            )
            ?? throw new Exception(
                "KAFKA_GROUP_ID missing"
            );
    }


    public void Start()
    {
        ConsumerConfig config = new()
        {
            BootstrapServers =
                _bootstrapServers,

            GroupId =
                _groupId,

            AutoOffsetReset =
                AutoOffsetReset.Earliest,

            EnableAutoCommit =
                false
        };


        using IConsumer<Ignore, string> consumer =
            new ConsumerBuilder<Ignore, string>(
                config
            )
            .Build();


        consumer.Subscribe(
            _topic
        );


        Console.WriteLine(
            "Waiting for Kafka messages..."
        );


        while (true)
        {
            try
            {
                ConsumeResult<Ignore, string> result =
                    consumer.Consume();


                EventMessage? message =
                    JsonSerializer.Deserialize<EventMessage>(
                        result.Message.Value,
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
                    $"Kafka received: {message.Id}"
                );


                _elasticService
                    .SaveAsync(message)
                    .GetAwaiter()
                    .GetResult();


                consumer.Commit(
                    result
                );
            }
            catch (Exception error)
            {
                Console.WriteLine(
                    $"Kafka Error: {error.Message}"
                );
            }
        }
        //ElasticsearchConsumerKafkaCSharpProgram
        
    }
}