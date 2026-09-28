using ElasticsearchConsumerCSharp.Services;


IElasticService elasticService =
    new ElasticService();


// --------------------------------------------------
// RabbitMQ
// --------------------------------------------------

IRabbitConsumerService rabbitConsumer =
    new RabbitConsumerService(
        elasticService
    );

rabbitConsumer.Start();


// --------------------------------------------------
// Kafka
// כרגע בהערה
// --------------------------------------------------

// IKafkaConsumerService kafkaConsumer =
//     new KafkaConsumerService(
//         elasticService
//     );

// kafkaConsumer.Start();
