using System.Text.Json;
using Producer.Models;

namespace Producer.Services;

public class DataReaderService : IDataReaderService
{
    public List<EventMessage> ReadJson(string path)
    {
        string json =
            File.ReadAllText(path);


        List<EventMessage>? messages =
            JsonSerializer.Deserialize<List<EventMessage>>(
                json,
                new JsonSerializerOptions
                {
                    PropertyNameCaseInsensitive = true
                }
            );


        return messages ?? new List<EventMessage>();
    }


    public List<EventMessage> ReadCsv(string path)
    {
        string[] lines =
            File.ReadAllLines(path);


        List<EventMessage> messages = new();


        foreach (string line in lines.Skip(1))
        {
            string[] values =
                line.Split(',');


            EventMessage message = new()
            {
                Id = values[0],
                Server = values[1],
                Type = values[2],
                Value = int.Parse(values[3])
            };


            messages.Add(message);
        }


        return messages;
    }
}