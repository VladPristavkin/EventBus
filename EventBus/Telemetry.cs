using System.Diagnostics;
using OpenTelemetry.Context.Propagation;

namespace EventBus
{
    public class Telemetry
    {
        public static string ActivitySourceName = "EventBusRabbitMQ";

        public ActivitySource ActivitySource { get; } = new(ActivitySourceName);
        public TextMapPropagator Propagator { get; } = Propagators.DefaultTextMapPropagator;
    }
}
