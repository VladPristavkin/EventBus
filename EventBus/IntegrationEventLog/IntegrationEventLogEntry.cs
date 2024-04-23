using EventBus.Events;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Text.Json;

namespace EventBus.IntegrationEventLog;

public class IntegrationEventLogEntry
{
    private static readonly JsonSerializerOptions s_indentedOptions = new() { WriteIndented = true };
    private static readonly JsonSerializerOptions s_caseInsensitiveOptions = new() { PropertyNameCaseInsensitive = true };

    private IntegrationEventLogEntry() { }

    public IntegrationEventLogEntry(IntegrationEvent @event) : this(@event, Guid.Empty) { }

    public IntegrationEventLogEntry(IntegrationEvent @event, Guid transactionId)
    {
        EventId = @event.Id;
        CreationTime = @event.CreationDate;
        EventTypeName = @event.GetType().FullName;
        Content = JsonSerializer.Serialize(@event, @event.GetType(), s_indentedOptions);
        State = EventStateEnum.NotPublished;
        TimesSent = 0;
        TransactionId = transactionId;
    }

    /// <summary>
    /// The unique identifier of the event.
    /// </summary>
    public Guid EventId { get; private set; }

    /// <summary>
    /// The full name of the event type.
    /// </summary>
    [Required]
    public string EventTypeName { get; private set; }

    /// <summary>
    /// The shortened name of the event type (class name without namespace).
    /// </summary>
    [NotMapped]
    public string EventTypeShortName => EventTypeName.Split('.')?.Last();

    /// <summary>
    /// The deserialized event object from the Content.
    /// </summary>
    [NotMapped]
    public IntegrationEvent IntegrationEvent { get; private set; }

    /// <summary>
    /// The state of the event (NotPublished, InProgress, Published, Failed).
    /// </summary>
    public EventStateEnum State { get; set; }

    /// <summary>
    /// The number of times the event has been sent.
    /// </summary>
    public int TimesSent { get; set; }

    /// <summary>
    /// The creation time of the event entry.
    /// </summary>
    public DateTime CreationTime { get; private set; }

    /// <summary>
    /// The serialized content of the event in JSON format.
    /// </summary>
    [Required]
    public string Content { get; private set; }

    /// <summary>
    /// The identifier of the transaction associated with the event.
    /// </summary>
    public Guid TransactionId { get; private set; }

    /// <summary>
    /// Deserializes the Content into an IntegrationEvent object based on the specified type.
    /// </summary>
    public IntegrationEventLogEntry DeserializeJsonContent(Type type)
    {
        IntegrationEvent = JsonSerializer.Deserialize(Content, type, s_caseInsensitiveOptions) as IntegrationEvent;
        return this;
    }
}
