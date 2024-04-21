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

        public static IEventBusBuilder AddRabbitMqEventBus(this IHostApplicationBuilder builder)
        {
            ArgumentNullException.ThrowIfNull(builder);

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

            builder.Services.AddOpenTelemetry()
               .WithTracing(tracing =>
               {
                   tracing.AddSource(Telemetry.ActivitySourceName);
               });

            builder.Services.Configure<EventBusOptions>(builder.Configuration.GetSection(SectionName));

            builder.Services.AddSingleton<Telemetry>();
            builder.Services.AddSingleton<IEventBus, EventBus>();

            builder.Services.AddSingleton<IHostedService>(sp => (EventBus)sp.GetRequiredService<IEventBus>());

            return new EventBusBuilder(builder.Services);
        }

        private class EventBusBuilder(IServiceCollection services) : IEventBusBuilder
        {
            public IServiceCollection Services => services;
        }
    }
}
