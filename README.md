# EventBus

This repository contains the code for a Event Bus implementation, utilizing RabbitMQ as the underlying messaging system. It facilitates asynchronous communication between different components of your application.

## Getting Started

### Dependencies
Ensure you have RabbitMQ installed and running.

### Installation
Integrate the EventBus library into your project.

### Configuration
Configure the RabbitMQ connection settings and event bus options.

### Define Events
Create classes representing your integration events inheriting from `IntegrationEvent`.

### Implement Handlers
Implement `IIntegrationEventHandler<T>` for each event type, defining the event handling logic.

### Register Subscriptions
Use `AddSubscription<TEvent, THandler>` to register event handlers with the event bus.

### Publish Events
Utilize `PublishAsync(event)` to publish events to the event bus.

## Usage Example

```csharp
// Define an event
public record OrderCreatedEvent : IntegrationEvent
{
    public int OrderId { get; init; }
}

// Implement an event handler
public class OrderCreatedEventHandler : IIntegrationEventHandler<OrderCreatedEvent>
{
    public async Task Handle(OrderCreatedEvent @event)
    {
        // Process the order created event
    }
}

// Register the event handler
services.AddRabbitMqEventBus()
    .AddSubscription<OrderCreatedEvent, OrderCreatedEventHandler>();

// Publish an event
await eventBus.PublishAsync(new OrderCreatedEvent { OrderId = 123 });
```

## License

This project is licensed under the MIT License.