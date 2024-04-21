using EventBus.Events;

namespace EventBus.Interfaces
{
    public interface IIntegrationEventHandler<in IIntegrationEvent> : IIntegrationEventHandler
        where IIntegrationEvent : IntegrationEvent
    {
        Task Handle(IIntegrationEvent @event);

        Task IIntegrationEventHandler.Handle(IntegrationEvent @event)
            => Handle((IIntegrationEvent)@event);
    }

    public interface IIntegrationEventHandler
    {
        Task Handle(IntegrationEvent @event);
    }
}
