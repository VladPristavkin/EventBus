using EventBus.Events;
using EventBus.Interfaces;
using Microsoft.Extensions.DependencyInjection;
using System.Diagnostics.CodeAnalysis;
using System.Text.Json;

namespace EventBus.Extensions
{
    public static class EventBusBuilderExtensions
    {
        /// <summary>
        /// Configures the JSON serialization options for the EventBus.
        /// </summary>
        /// <param name="eventBusBuilder">The EventBus builder.</param>
        /// <param name="configure">The action to configure the JSON serialization options.</param>
        /// <returns>The EventBus builder.</returns>
        public static IEventBusBuilder ConfigureJsonOptions(this IEventBusBuilder eventBusBuilder, Action<JsonSerializerOptions> configure)
        {
            eventBusBuilder.Services.Configure<EventBusSubscriptionInfo>(o =>
            {
                configure(o.JsonSerializerOptions);
            });

            return eventBusBuilder;
        }

        /// <summary>
        /// Adds a subscription for an integration event.
        /// </summary>
        /// <typeparam name="T">The type of the integration event.</typeparam>
        /// <typeparam name="TH">The type of the integration event handler.</typeparam>
        /// <param name="eventBusBuilder">The EventBus builder.</param>
        /// <returns>The EventBus builder.</returns>
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
