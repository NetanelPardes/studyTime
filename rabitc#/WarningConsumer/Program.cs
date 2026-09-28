using WarningConsumer.Services;
IRabbitConsumerService consumer = new RabbitConsumerService();
consumer.Start();