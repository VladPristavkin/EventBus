using EventBus.Interfaces;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Client;

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
            builder.Services.Configure<EventBusOptions>(builder.Configuration.GetSection(SectionName));

            // Register Telemetry and EventBus implementations
            builder.Services.AddSingleton<Telemetry>();
            builder.Services.AddSingleton<IEventBus, EventBus>();

            // Register EventBus as a hosted service
            builder.Services.AddSingleton<IHostedService>(sp => (EventBus)sp.GetRequiredService<IEventBus>());

            // Return EventBusBuilder for further configuration
            return new EventBusBuilder(builder.Services);
        }

        private class EventBusBuilder(IServiceCollection services) : IEventBusBuilder
        {
            public IServiceCollection Services => services;
        }
    }
}
