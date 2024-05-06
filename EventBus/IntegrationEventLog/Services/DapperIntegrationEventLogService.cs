using Dapper;
using EventBus.Events;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using System.Data.Common;
using System.Reflection;

namespace EventBus.IntegrationEventLog.Services
{
    public class DapperIntegrationEventLogService :
        IIntegrationEventLogService, IDisposable
    {
        private volatile bool _disposedValue;
        private readonly DbConnection _dbConnection;
        private readonly Type[] _eventTypes;

        public DapperIntegrationEventLogService(DbConnection dbConnection)
        {
            _dbConnection = dbConnection;
            _eventTypes = Assembly.Load(Assembly.GetEntryAssembly().FullName)
                .GetTypes()
                .Where(t => t.Name.EndsWith(nameof(IntegrationEvent)))
                .ToArray();
        }

        /// <summary>
        /// Retrieves a collection of integration event log entries that are failed.
        /// </summary>
        /// <returns>An enumerable collection of <see cref="IntegrationEventLogEntry"/> objects.</returns>
        public async Task<IEnumerable<IntegrationEventLogEntry>> RetrievingLogsOfFailedEventsAsync()
        {
            var sql = @"SELECT 
                        EventId, 
                        EventTypeName,
                        State,
                        TimesSent,
                        CreationTime,
                        Content,
                        TransactionId
                    FROM IntegrationEventLog
                    WHERE State = @FailedState
                    ORDER BY CreationTime";

            var result = await _dbConnection.QueryAsync<IntegrationEventLogEntry>(sql, new
            {
                FailedState = (int)EventStateEnum.PublishedFailed
            });

            if (result.Any())
            {
                return result.Select(e => e.DeserializeJsonContent(_eventTypes.FirstOrDefault(t => t.Name == e.EventTypeShortName)));
            }

            return Enumerable.Empty<IntegrationEventLogEntry>();
        }

        /// <summary>
        /// Retrieves a collection of integration event log entries that are failed and belong to the specified transaction.
        /// </summary>
        /// <returns>An enumerable collection of <see cref="IntegrationEventLogEntry"/> objects.</returns>
        public async Task<IEnumerable<IntegrationEventLogEntry>> RetrievingLogsOfFailedEventsAsync(Guid transactionId)
        {
            var sql = @"SELECT 
                        EventId,
                        EventTypeName,
                        State,
                        TimesSent,
                        CreationTime,
                        Content,
                        TransactionId
                    FROM IntegrationEventLog
                    WHERE TransactionId = @TransactionId AND State = @FailedState
                    ORDER BY CreationTime";

            var result = await _dbConnection.QueryAsync<IntegrationEventLogEntry>(sql, new
            {
                TransactionId = transactionId,
                FailedState = (int)EventStateEnum.PublishedFailed
            });

            if (result.Any())
            {
                return result.Select(e => e.DeserializeJsonContent(_eventTypes.FirstOrDefault(t => t.Name == e.EventTypeShortName)));
            }

            return Enumerable.Empty<IntegrationEventLogEntry>();
        }

        /// <summary>
        /// Retrieves a collection of integration event log entries that are pending to be published.
        /// </summary>
        /// <returns>An enumerable collection of <see cref="IntegrationEventLogEntry"/> objects.</returns>
        public async Task<IEnumerable<IntegrationEventLogEntry>> RetrieveEventLogsPendingToPublishAsync()
        {
            var sql = @"SELECT 
                            EventId, 
                            EventTypeName, 
                            State, 
                            TimesSent, 
                            CreationTime, 
                            Content, 
                            TransactionId 
                        FROM IntegrationEventLog
                        WHERE State = @State
                        ORDER BY CreationTime";

            var result = await _dbConnection.QueryAsync<IntegrationEventLogEntry>(sql,
                new { State = (int)EventStateEnum.NotPublished });

            if (result.Any())
            {
                return result.Select(e => e.DeserializeJsonContent(_eventTypes.FirstOrDefault(t => t.Name == e.EventTypeShortName)));
            }

            return Enumerable.Empty<IntegrationEventLogEntry>();
        }

        /// <summary>
        /// Retrieves a collection of integration event log entries that are pending to be published and belong to the specified transaction.
        /// </summary>
        /// <returns>An enumerable collection of <see cref="IntegrationEventLogEntry"/> objects.</returns>
        public async Task<IEnumerable<IntegrationEventLogEntry>> RetrieveEventLogsPendingToPublishAsync(Guid transactionId)
        {
            var sql = @"SELECT 
                        EventId, 
                        EventTypeName, 
                        State, 
                        TimesSent, 
                        CreationTime, 
                        Content, 
                        TransactionId 
                    FROM IntegrationEventLog
                    WHERE TransactionId = @TransactionId AND State = @State
                    ORDER BY CreationTime";

            var result = await _dbConnection.QueryAsync<IntegrationEventLogEntry>(sql,
                new
                {
                    TransactionId = transactionId,
                    State = (int)EventStateEnum.NotPublished
                });

            if (result.Any())
            {
                return result.Select(e => e.DeserializeJsonContent(_eventTypes.FirstOrDefault(t => t.Name == e.EventTypeShortName)));
            }

            return Enumerable.Empty<IntegrationEventLogEntry>();
        }

        /// <summary>
        /// Saves the specified integration event to the event log.
        /// </summary>
        public async Task SaveEventAsync(IntegrationEvent @event)
        {
            var eventLogEntry = new IntegrationEventLogEntry(@event);

            var sql = @"
        INSERT INTO IntegrationEventLog 
            (EventId, EventTypeName, State, TimesSent, CreationTime, Content, TransactionId)
        VALUES 
            (@EventId, @EventTypeName, @State, @TimesSent, @CreationTime, @Content, @TransactionId)";

            await _dbConnection.ExecuteAsync(sql, new
            {
                eventLogEntry.EventId,
                eventLogEntry.EventTypeName,
                State = (int)eventLogEntry.State,
                eventLogEntry.TimesSent,
                eventLogEntry.CreationTime,
                eventLogEntry.Content,
                eventLogEntry.TransactionId
            });
        }

        /// <summary>
        /// Saves the specified integration event to the event log within the context of the specified transaction.
        /// </summary>
        public async Task SaveEventAsync(IntegrationEvent @event, IDbContextTransaction transaction)
        {
            ArgumentNullException.ThrowIfNull(transaction);

            var eventLogEntry = new IntegrationEventLogEntry(@event, transaction.TransactionId);

            var sql = @"
        INSERT INTO IntegrationEventLog 
            (EventId, EventTypeName, State, TimesSent, CreationTime, Content, TransactionId)
        VALUES 
            (@EventId, @EventTypeName, @State, @TimesSent, @CreationTime, @Content, @TransactionId)";

            await _dbConnection.ExecuteAsync(sql, new
            {
                eventLogEntry.EventId,
                eventLogEntry.EventTypeName,
                State = (int)eventLogEntry.State,
                eventLogEntry.TimesSent,
                eventLogEntry.CreationTime,
                eventLogEntry.Content,
                eventLogEntry.TransactionId
            }, (System.Data.IDbTransaction?)transaction);
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

        private async Task UpdateEventStatus(Guid eventId, EventStateEnum status)
        {
            var sql = @"
        UPDATE IntegrationEventLog
        SET 
            State = @Status,
            TimesSent = CASE WHEN @Status = @InProgressState THEN TimesSent + 1 ELSE TimesSent END
        WHERE EventId = @EventId";

            await _dbConnection.ExecuteAsync(sql, new
            {
                EventId = eventId,
                Status = (int)status,
                InProgressState = (int)EventStateEnum.InProgress
            });
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposedValue)
            {
                if (disposing)
                {
                    _dbConnection.Dispose();
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
}
