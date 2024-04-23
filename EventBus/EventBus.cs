using EventBus.Events;
using EventBus.Interfaces;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using OpenTelemetry;
using OpenTelemetry.Context.Propagation;
using Polly;
using Polly.Retry;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;

namespace EventBus
{
    /// <summary>
    /// Represents an event bus implementation using RabbitMQ.
    /// </summary>
    public sealed class EventBus(
        ILogger<EventBus> logger,
        IServiceProvider serviceProvider,
        IOptions<EventBusOptions> options,
        IOptions<EventBusSubscriptionInfo> subscriptionOptions,
        Telemetry Telemetry) : IEventBus, IDisposable, IHostedService
    {
        private const string ExchangeName = "it-intern_event_bus";

        private readonly ResiliencePipeline _pipeline = CreateResiliencePipeline(options.Value.RetryCount);
        private readonly TextMapPropagator _propagator = Telemetry.Propagator;
        private readonly ActivitySource _activitySource = Telemetry.ActivitySource;
        private readonly string _queueName = options.Value.SubscriptionClientName;
        private readonly EventBusSubscriptionInfo _subscriptionInfo = subscriptionOptions.Value;
        private IConnection _rabbitMQConnection;
        private IModel _consumerChannel;

        /// <summary>
        /// Publishes an integration event to the event bus.
        /// </summary>
        /// <param name="event">The integration event to publish.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        public Task PublishAsync(IntegrationEvent @event)
        {
            var routingKey = @event.GetType().Name;

            if (logger.IsEnabled(LogLevel.Trace))
            {
                logger.LogTrace("Creating RabbitMQ channel to publish event: {EventId} ({EventName})", @event.Id, routingKey);
            }

            using var channel = _rabbitMQConnection?.CreateModel() ?? throw new InvalidOperationException("RabbitMQ connection is not open");

            if (logger.IsEnabled(LogLevel.Trace))
            {
                logger.LogTrace("Declaring RabbitMQ exchange to publish event: {EventId}", @event.Id);
            }

            channel.ExchangeDeclare(exchange: ExchangeName, type: "direct");

            var body = SerializeMessage(@event);

            var activityName = $"{routingKey} publish";

            return _pipeline.Execute(() =>
            {
                using var activity = _activitySource.StartActivity(activityName, ActivityKind.Client);

                ActivityContext contextToInject = default;

                if (activity != null)
                {
                    contextToInject = activity.Context;
                }
                else if (Activity.Current != null)
                {
                    contextToInject = Activity.Current.Context;
                }

                var properties = channel.CreateBasicProperties();

                properties.DeliveryMode = 2;

                static void InjectTraceContextIntoBasicProperties(IBasicProperties props, string key, string value)
                {
                    props.Headers ??= new Dictionary<string, object>();
                    props.Headers[key] = value;
                }

                _propagator.Inject(new PropagationContext(contextToInject, Baggage.Current), properties, InjectTraceContextIntoBasicProperties);

                SetActivityContext(activity, routingKey, "publish");

                if (logger.IsEnabled(LogLevel.Trace))
                {
                    logger.LogTrace("Publishing event to RabbitMQ: {EventId}", @event.Id);
                }

                try
                {
                    channel.BasicPublish(
                        exchange: ExchangeName,
                        routingKey: routingKey,
                        mandatory: true,
                        basicProperties: properties,
                        body: body);

                    return Task.CompletedTask;
                }
                catch (Exception ex)
                {
                    SetExceptionTags(activity, ex);

                    throw;
                }
            });
        }

        /// <summary>
        /// Sets the activity context with RabbitMQ-specific tags.
        /// </summary>
        /// <param name="activity">The activity to set the context for.</param>
        /// <param name="routingKey">The RabbitMQ routing key.</param>
        /// <param name="operation">The messaging operation (e.g., "receive", "publish").</param>
        private static void SetActivityContext(Activity activity, string routingKey, string operation)
        {
            if (activity is not null)
            {
                activity.SetTag("messaging.system", "rabbitmq");
                activity.SetTag("messaging.destination_kind", "queue");
                activity.SetTag("messaging.operation", operation);
                activity.SetTag("messaging.destination.name", routingKey);
                activity.SetTag("messaging.rabbitmq.routing_key", routingKey);
            }
        }

        /// <summary>
        /// Disposes the resources used by the event bus.
        /// </summary>
        public void Dispose()
        {
            _consumerChannel?.Dispose();
        }

        /// <summary>
        /// Handles a message received from RabbitMQ.
        /// </summary>
        /// <param name="sender">The source of the event.</param>
        /// <param name="eventArgs">The event arguments containing the message details.</param>
        private async Task OnMessageReceived(object sender, BasicDeliverEventArgs eventArgs)
        {
            static IEnumerable<string> ExtractTraceContextFromBasicProperties(IBasicProperties props, string key)
            {
                if (props.Headers.TryGetValue(key, out var value))
                {
                    var bytes = value as byte[];
                    return [Encoding.UTF8.GetString(bytes)];
                }
                return [];
            }

            var parentContext = _propagator.Extract(default, eventArgs.BasicProperties, ExtractTraceContextFromBasicProperties);
            Baggage.Current = parentContext.Baggage;

            var activityName = $"{eventArgs.RoutingKey} receive";

            using var activity = _activitySource.StartActivity(activityName, ActivityKind.Client, parentContext.ActivityContext);

            SetActivityContext(activity, eventArgs.RoutingKey, "receive");

            var eventName = eventArgs.RoutingKey;
            var message = Encoding.UTF8.GetString(eventArgs.Body.Span);

            try
            {
                activity?.SetTag("message", message);

                if (message.Contains("throw-fake-exception", StringComparison.InvariantCultureIgnoreCase))
                {
                    throw new InvalidOperationException($"Fake exception requested: \"{message}\"");
                }

                await ProcessEvent(eventName, message);
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "Error Processing message \"{Message}\"", message);

                SetExceptionTags(activity, ex);
            }

            _consumerChannel.BasicAck(eventArgs.DeliveryTag, multiple: false);
        }

        /// <summary>
        /// Sets the exception tags on the provided activity.
        /// </summary>
        /// <param name="activity">The activity to set the exception tags on.</param>
        /// <param name="ex">The exception.</param>
        private static void SetExceptionTags(Activity activity, Exception ex)
        {
            if (activity is null)
            {
                return;
            }

            activity.AddTag("exception.message", ex.Message);
            activity.AddTag("exception.stacktrace", ex.ToString());
            activity.AddTag("exception.type", ex.GetType().FullName);
            activity.SetStatus(ActivityStatusCode.Error);
        }

        /// <summary>
        /// Processes a received event.
        /// </summary>
        /// <param name="eventName">The name of the event.</param>
        /// <param name="message">The message content.</param>
        private async Task ProcessEvent(string eventName, string message)
        {
            if (logger.IsEnabled(LogLevel.Trace))
            {
                logger.LogTrace("Processing RabbitMQ event: {EventName}", eventName);
            }

            await using var scope = serviceProvider.CreateAsyncScope();

            if (!_subscriptionInfo.EventTypes.TryGetValue(eventName, out var eventType))
            {
                logger.LogWarning("Unable to resolve event type for event name {EventName}", eventName);
                return;
            }

            var integrationEvent = DeserializeMessage(message, eventType);

            foreach (var handler in scope.ServiceProvider.GetKeyedServices<IIntegrationEventHandler>(eventType))
            {
                await handler.Handle(integrationEvent);
            }
        }

        [UnconditionalSuppressMessage("Trimming", "IL2026:RequiresUnreferencedCode",
            Justification = "The 'JsonSerializer.IsReflectionEnabledByDefault' feature switch, which is set to false by default for trimmed .NET apps, ensures the JsonSerializer doesn't use Reflection.")]
        [UnconditionalSuppressMessage("AOT", "IL3050:RequiresDynamicCode", Justification = "See above.")]
        private IntegrationEvent DeserializeMessage(string message, Type eventType)
        {
            return JsonSerializer.Deserialize(message, eventType, _subscriptionInfo.JsonSerializerOptions) as IntegrationEvent;
        }

        [UnconditionalSuppressMessage("Trimming", "IL2026:RequiresUnreferencedCode",
            Justification = "The 'JsonSerializer.IsReflectionEnabledByDefault' feature switch, which is set to false by default for trimmed .NET apps, ensures the JsonSerializer doesn't use Reflection.")]
        [UnconditionalSuppressMessage("AOT", "IL3050:RequiresDynamicCode", Justification = "See above.")]
        private byte[] SerializeMessage(IntegrationEvent @event)
        {
            return JsonSerializer.SerializeToUtf8Bytes(@event, @event.GetType(), _subscriptionInfo.JsonSerializerOptions);
        }

        /// <summary>
        /// Starts the event bus and begins consuming messages from RabbitMQ.
        /// </summary>
        /// <param name="cancellationToken">A cancellation token that can be used to cancel the asynchronous operation.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        public Task StartAsync(CancellationToken cancellationToken)
        {
            _ = Task.Factory.StartNew(() =>
            {
                try
                {
                    logger.LogInformation("Starting RabbitMQ connection on a background thread");

                    _rabbitMQConnection = serviceProvider.GetRequiredService<IConnection>();
                    if (!_rabbitMQConnection.IsOpen)
                    {
                        return;
                    }

                    if (logger.IsEnabled(LogLevel.Trace))
                    {
                        logger.LogTrace("Creating RabbitMQ consumer channel");
                    }

                    _consumerChannel = _rabbitMQConnection.CreateModel();

                    _consumerChannel.CallbackException += (sender, ea) =>
                    {
                        logger.LogWarning(ea.Exception, "Error with RabbitMQ consumer channel");
                    };

                    _consumerChannel.ExchangeDeclare(exchange: ExchangeName,
                                            type: "direct");

                    _consumerChannel.QueueDeclare(queue: _queueName,
                                         durable: true,
                                         exclusive: false,
                                         autoDelete: false,
                                         arguments: null);

                    if (logger.IsEnabled(LogLevel.Trace))
                    {
                        logger.LogTrace("Starting RabbitMQ basic consume");
                    }

                    var consumer = new AsyncEventingBasicConsumer(_consumerChannel);

                    consumer.Received += OnMessageReceived;

                    _consumerChannel.BasicConsume(
                        queue: _queueName,
                        autoAck: false,
                        consumer: consumer);

                    foreach (var (eventName, _) in _subscriptionInfo.EventTypes)
                    {
                        _consumerChannel.QueueBind(
                            queue: _queueName,
                            exchange: ExchangeName,
                            routingKey: eventName);
                    }
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "Error starting RabbitMQ connection");
                }
            },
            TaskCreationOptions.LongRunning);

            return Task.CompletedTask;
        }

        /// <summary>
        /// Stops the event bus and stops consuming messages from RabbitMQ.
        /// </summary>
        /// <param name="cancellationToken">A cancellation token that can be used to cancel the asynchronous operation.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        public Task StopAsync(CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }

        /// <summary>
        /// Creates a resilience pipeline for handling RabbitMQ exceptions and retrying the operation.
        /// </summary>
        /// <param name="retryCount">The number of retry attempts.</param>
        /// <returns>The resilience pipeline.</returns>
        private static ResiliencePipeline CreateResiliencePipeline(int retryCount)
        {
            var retryOptions = new RetryStrategyOptions
            {
                ShouldHandle = new PredicateBuilder()
                .Handle<BrokerUnreachableException>()
                .Handle<SocketException>(),

                MaxRetryAttempts = retryCount,
                DelayGenerator = (context) =>
                ValueTask.FromResult(GenerateDelay(context.AttemptNumber))
            };

            return new ResiliencePipelineBuilder()
                .AddRetry(retryOptions)
                .Build();

            static TimeSpan? GenerateDelay(int attempt)
            {
                return TimeSpan.FromSeconds(Math.Pow(2, attempt));
            }
        }
    }
}
