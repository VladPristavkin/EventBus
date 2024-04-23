using System.Diagnostics;
using OpenTelemetry.Context.Propagation;

namespace EventBus
{
    public class Telemetry
    {
        /// <summary>
        /// The name of the ActivitySource for EventBusRabbitMQ.
        /// </summary>
        public static string ActivitySourceName = "EventBusRabbitMQ";

        /// <summary>
        /// The ActivitySource for EventBusRabbitMQ.
        /// Used for tracking and monitoring activities within the application.
        public ActivitySource ActivitySource { get; } = new(ActivitySourceName);

        /// <summary>
        /// The TextMapPropagator for propagating context across different components of the application.
        /// Used for correlating activities and propagating context across different components and services.
        /// </summary>
        public TextMapPropagator Propagator { get; } = Propagators.DefaultTextMapPropagator;
    }
}
