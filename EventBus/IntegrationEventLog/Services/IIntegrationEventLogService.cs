using EventBus.Events;
using Microsoft.EntityFrameworkCore.Storage;

namespace EventBus.IntegrationEventLog.Services;

public interface IIntegrationEventLogService
{
    /// <summary>
    /// Retrieves a collection of integration event log entries that are pending to be published.
    /// </summary>
    /// <returns>An enumerable collection of <see cref="IntegrationEventLogEntry"/> objects.</returns>
    Task<IEnumerable<IntegrationEventLogEntry>> RetrieveEventLogsPendingToPublishAsync();

    /// <summary>
    /// Retrieves a collection of integration event log entries that are pending to be published and belong to the specified transaction.
    /// </summary>
    /// <returns>An enumerable collection of <see cref="IntegrationEventLogEntry"/> objects.</returns>
    Task<IEnumerable<IntegrationEventLogEntry>> RetrieveEventLogsPendingToPublishAsync(Guid transactionId);

    /// <summary>
    /// Saves the specified integration event to the event log.
    /// </summary>
    Task SaveEventAsync(IntegrationEvent @event);

    /// <summary>
    /// Saves the specified integration event to the event log within the context of the specified transaction.
    /// </summary>
    Task SaveEventAsync(IntegrationEvent @event, IDbContextTransaction transaction);

    /// <summary>
    /// Marks the integration event log entry with the specified ID as published.
    /// </summary>
    Task MarkEventAsPublishedAsync(Guid eventId);

    /// <summary>
    /// Marks the integration event log entry with the specified ID as in progress.
    /// </summary>
    Task MarkEventAsInProgressAsync(Guid eventId);

    /// <summary>
    /// Marks the integration event log entry with the specified ID as failed.
    /// </summary>
    Task MarkEventAsFailedAsync(Guid eventId);
}
