using EventBus.Events;
using EventBus.Interfaces;
using Microsoft.Extensions.DependencyInjection;
using System.Diagnostics.CodeAnalysis;
using System.Text.Json;

namespace EventBus.Extensions
{
    public static class EventBusBuilderExtensions
    {
        public static IEventBusBuilder ConfigureJsonOptions(this IEventBusBuilder eventBusBuilder, Action<JsonSerializerOptions> configure)
        {
            eventBusBuilder.Services.Configure<EventBusSubscriptionInfo>(o =>
            {
                configure(o.JsonSerializerOptions);
            });

            return eventBusBuilder;
        }

        public static IEventBusBuilder AddSubscription<T, [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TH>(this IEventBusBuilder eventBusBuilder)
            where T : IntegrationEvent
            where TH : class, IIntegrationEventHandler<T>
        {
            eventBusBuilder.Services.AddKeyedTransient<IIntegrationEventHandler, TH>(typeof(T));

            eventBusBuilder.Services.Configure<EventBusSubscriptionInfo>(o =>
            {
                o.EventTypes[typeof(T).Name] = typeof(T);
            });

            return eventBusBuilder;
        }
    }
}
