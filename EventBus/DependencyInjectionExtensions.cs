using EventBus.Events;
using EventBus.IntegrationEventLog.Services;
using EventBus.Interfaces;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Client;
using System.Diagnostics.CodeAnalysis;
using System.Text.Json;

namespace EventBus
{
    public static class DependencyInjectionExtensions
    {
        private const string SectionName = "EventBus";

        /// <summary>
        /// Adds RabbitMQ as the event bus implementation.
        /// </summary>
        /// <param name="builder">The host application builder.</param>
        /// <returns>An object that can be used to further configure the event bus.</returns>
        public static IEventBusBuilder AddRabbitMqEventBus(this IHostApplicationBuilder builder)
        {
            ArgumentNullException.ThrowIfNull(builder);

            // Configure RabbitMQ connection factory
            builder.Services.AddSingleton(sp =>
            {
                var factory = new ConnectionFactory
                {
                    DispatchConsumersAsync = true
                };

                builder.Configuration.GetSection(SectionName).Bind(factory);

                var connection = factory.CreateConnection();

                return connection;
            });

            // Add OpenTelemetry for tracing
            builder.Services.AddOpenTelemetry()
               .WithTracing(tracing =>
               {
                   tracing.AddSource(Telemetry.ActivitySourceName);
               });

            // Configure EventBusOptions
            builder.Services.Configure<EventBusOptions>(options =>
            {
                builder.Configuration.GetSection(SectionName).Bind(options);
            });

            // Register Telemetry and EventBus implementations
            builder.Services.AddSingleton<Telemetry>();
            builder.Services.AddSingleton<IEventBus, EventBus>();

            // Register EventBus as a hosted service
            builder.Services.AddSingleton<IHostedService>(sp => (EventBus)sp.GetRequiredService<IEventBus>());

            // Return EventBusBuilder for further configuration
            return new EventBusBuilder(builder.Services);
        }

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

        /// <summary>
        /// Extends IServiceCollection to add registration for an integration event log service.
        /// </summary>
        /// <param name="services">The collection of services to add the integration event log service to.</param>
        /// <param name="scoped">If true, the integration event log service is registered with scoped lifetime.</param>
        /// <param name="transient">If true, the integration event log service is registered with transient lifetime.</param>
        /// <param name="singleton">If true, the integration event log service is registered with singleton lifetime.</param>
        /// <returns>The modified IServiceCollection after adding the integration event log service registration.</returns>
        /// <exception cref="ArgumentNullException">Thrown when the services parameter is null.</exception>
        public static IServiceCollection AddIntegrationEventLogService<T>(
            this IServiceCollection services,
            bool scoped = false,
            bool transient = false,
            bool singleton = true)
            where T : class, IIntegrationEventLogService
        {
            ArgumentNullException.ThrowIfNull(services);

            if (singleton && !scoped && !transient)
                return services.AddSingleton<IIntegrationEventLogService, T>();

            if (scoped && !singleton && !transient)
                return services.AddScoped<IIntegrationEventLogService, T>();

            if (transient && !singleton && !scoped)
                return services.AddTransient<IIntegrationEventLogService, T>();

            else return services.AddSingleton<IIntegrationEventLogService, T>();
        }

        private class EventBusBuilder(IServiceCollection services) : IEventBusBuilder
        {
            public IServiceCollection Services => services;
        }
    }
}
