using EventBus.Events;

namespace EventBus.Interfaces
{
    public interface IEventBus
    {
        Task PublishAsync(IntegrationEvent @event);
    }
}
