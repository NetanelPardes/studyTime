namespace Producer.Models;

public class EventMessage
{
    public string? Id { get; set; }

    public string? Server { get; set; }

    public string? Type { get; set; }

    public int Value { get; set; }

    public string? Status { get; set; }
}