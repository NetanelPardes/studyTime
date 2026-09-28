using CriticalConsumer.Services;
IRabbitConsumerService consumer = new RabbitConsumerService();
consumer.Start();
