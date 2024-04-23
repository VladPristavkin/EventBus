using EventBus.Events;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using System.Reflection;

namespace EventBus.IntegrationEventLog.Services;

public class IntegrationEventLogService<TContext> : IIntegrationEventLogService, IDisposable
    where TContext : DbContext
{
    private volatile bool _disposedValue;
    private readonly TContext _context;
    private readonly Type[] _eventTypes;

    public IntegrationEventLogService(TContext context)
    {
        _context = context;
        _eventTypes = Assembly.Load(Assembly.GetEntryAssembly().FullName)
            .GetTypes()
            .Where(t => t.Name.EndsWith(nameof(IntegrationEvent)))
            .ToArray();
    }

    /// <summary>
    /// Retrieves a collection of integration event log entries that are pending to be published.
    /// </summary>
    /// <returns>An enumerable collection of <see cref="IntegrationEventLogEntry"/> objects.</returns>
    public async Task<IEnumerable<IntegrationEventLogEntry>> RetrieveEventLogsPendingToPublishAsync()
    {
        var result = await _context.Set<IntegrationEventLogEntry>()
            .Where(e => e.State == EventStateEnum.NotPublished)
            .ToListAsync();

        if (result.Count != 0)
        {
            return result.OrderBy(o => o.CreationTime)
                .Select(e => e.DeserializeJsonContent(_eventTypes.FirstOrDefault(t => t.Name == e.EventTypeShortName)));
        }

        return [];
    }

    /// <summary>
    /// Retrieves a collection of integration event log entries that are pending to be published and belong to the specified transaction.
    /// </summary>
    /// <returns>An enumerable collection of <see cref="IntegrationEventLogEntry"/> objects.</returns>
    public async Task<IEnumerable<IntegrationEventLogEntry>> RetrieveEventLogsPendingToPublishAsync(Guid transactionId)
    {
        var result = await _context.Set<IntegrationEventLogEntry>()
            .Where(e => e.TransactionId == transactionId && e.State == EventStateEnum.NotPublished)
            .ToListAsync();

        if (result.Count != 0)
        {
            return result.OrderBy(o => o.CreationTime)
                .Select(e => e.DeserializeJsonContent(_eventTypes.FirstOrDefault(t => t.Name == e.EventTypeShortName)));
        }

        return [];
    }

    /// <summary>
    /// Saves the specified integration event to the event log.
    /// </summary>
    public Task SaveEventAsync(IntegrationEvent @event)
    {
        var eventLogEntry = new IntegrationEventLogEntry(@event);

        _context.Set<IntegrationEventLogEntry>().Add(eventLogEntry);

        return _context.SaveChangesAsync();
    }

    /// <summary>
    /// Saves the specified integration event to the event log within the context of the specified transaction.
    /// </summary>
    public Task SaveEventAsync(IntegrationEvent @event, IDbContextTransaction transaction)
    {
        ArgumentNullException.ThrowIfNull(transaction);

        var eventLogEntry = new IntegrationEventLogEntry(@event, transaction.TransactionId);

        _context.Database.UseTransaction(transaction.GetDbTransaction());
        _context.Set<IntegrationEventLogEntry>().Add(eventLogEntry);

        return _context.SaveChangesAsync();
    }

    /// <summary>
    /// Marks the integration event log entry with the specified ID as published.
    /// </summary>
    public Task MarkEventAsPublishedAsync(Guid eventId)
    {
        return UpdateEventStatus(eventId, EventStateEnum.Published);
    }

    /// <summary>
    /// Marks the integration event log entry with the specified ID as in progress.
    /// </summary>
    public Task MarkEventAsInProgressAsync(Guid eventId)
    {
        return UpdateEventStatus(eventId, EventStateEnum.InProgress);
    }

    /// <summary>
    /// Marks the integration event log entry with the specified ID as failed.
    /// </summary>
    public Task MarkEventAsFailedAsync(Guid eventId)
    {
        return UpdateEventStatus(eventId, EventStateEnum.PublishedFailed);
    }

    private Task UpdateEventStatus(Guid eventId, EventStateEnum status)
    {
        var eventLogEntry = _context.Set<IntegrationEventLogEntry>().Single(ie => ie.EventId == eventId);
        eventLogEntry.State = status;

        if (status == EventStateEnum.InProgress)
            eventLogEntry.TimesSent++;

        return _context.SaveChangesAsync();
    }

    protected virtual void Dispose(bool disposing)
    {
        if (!_disposedValue)
        {
            if (disposing)
            {
                _context.Dispose();
            }

            _disposedValue = true;
        }
    }

    public void Dispose()
    {
        Dispose(disposing: true);
        GC.SuppressFinalize(this);
    }
}
