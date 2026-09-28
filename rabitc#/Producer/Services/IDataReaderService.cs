using Producer.Models;

namespace Producer.Services;

public interface IDataReaderService
{
    List<EventMessage> ReadJson(string path);

    List<EventMessage> ReadCsv(string path);
}