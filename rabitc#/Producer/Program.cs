using Producer.Models;
using Producer.Services;


IDataReaderService dataReader =
    new DataReaderService();


IRabbitProducerService producer =
    new RabbitProducerService();


string jsonPath = Path.Combine(
    AppContext.BaseDirectory,
    "events.json"
);


string csvPath = Path.Combine(
    AppContext.BaseDirectory,
    "events.csv"
);


List<EventMessage> jsonMessages =dataReader.ReadJson(jsonPath);
foreach (EventMessage message in jsonMessages)
{
    producer.SendMessage(message);
}
Console.WriteLine($"Finished JSON: {jsonMessages.Count} messages");

List<EventMessage> csvMessages = dataReader.ReadCsv(csvPath);


foreach (EventMessage message in csvMessages)
{
    producer.SendMessage(message);
}


Console.WriteLine(
    $"Finished CSV: {csvMessages.Count} messages"
);


Console.WriteLine(
    "Producer finished"
);