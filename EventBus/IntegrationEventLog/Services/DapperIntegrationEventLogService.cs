using Dapper;
using EventBus.Events;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using System.ComponentModel.DataAnnotations.Schema;
using System.ComponentModel.DataAnnotations;
using System.Data.Common;
using System.Reflection;
using System.Text.Json;

namespace EventBus.IntegrationEventLog.Services
{
    public class DapperIntegrationEventLogService :
        IIntegrationEventLogService
    {
        private volatile bool _disposedValue;
        private readonly string _dbConnectionString;
        private readonly string _providerName;
        private readonly Type[] _eventTypes;

        public DapperIntegrationEventLogService(string dbConnectionString,
            string providerName,
            Type? assemblyReference = null)
        {
            _dbConnectionString = dbConnectionString ?? throw new ArgumentNullException(nameof(dbConnectionString));
            _providerName = providerName ?? throw new ArgumentNullException(providerName);

            if (assemblyReference != null)
            {
                _eventTypes = Assembly.Load(Assembly.GetAssembly(assemblyReference).FullName)
                    .GetTypes()
                    .Where(t => t.Name.EndsWith(nameof(IntegrationEvent)))
                    .ToArray();
            }
            else
            {
                _eventTypes = Assembly.Load(Assembly.GetEntryAssembly().FullName)
                  .GetTypes()
                  .Where(t => t.Name.EndsWith(nameof(IntegrationEvent)))
                  .ToArray();
            }
        }

        /// <summary>
        /// Retrieves a collection of integration event log entries that are failed.
        /// </summary>
        /// <returns>An enumerable collection of <see cref="IntegrationEventLogEntry"/> objects.</returns>
        public async Task<IEnumerable<IntegrationEventLogEntry>> RetrievingLogsOfFailedEventsAsync()
        {
            const string sql = @"SELECT 
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

            using (var connection = EnsureCreateAndOpenConnection())
            {
                var result = await connection.QueryAsync<IntegrationEventLogEntryEntity>(sql, new
                {
                    FailedState = (int)EventStateEnum.PublishedFailed
                });

                var logEntries = result.Select(row => new IntegrationEventLogEntry
                (Guid.Parse(row.EventId), row.EventTypeName,
                (EventStateEnum)row.State,
                row.TimesSent,
                row.CreationTime,
                row.Content,
                Guid.Parse(row.TransactionId)
                )).ToList();

                return logEntries.Any() ?
                    logEntries.Select(e => e.DeserializeJsonContent(_eventTypes.FirstOrDefault(t => t.Name == e.EventTypeShortName))) :
                    Enumerable.Empty<IntegrationEventLogEntry>();
            }
        }

        /// <summary>
        /// Retrieves a collection of integration event log entries that are failed and belong to the specified transaction.
        /// </summary>
        /// <returns>An enumerable collection of <see cref="IntegrationEventLogEntry"/> objects.</returns>
        public async Task<IEnumerable<IntegrationEventLogEntry>> RetrievingLogsOfFailedEventsAsync(Guid transactionId)
        {
            const string sql = @"SELECT 
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

            using (var connection = EnsureCreateAndOpenConnection())
            {
                var result = await connection.QueryAsync<IntegrationEventLogEntryEntity>(sql, new
                {
                    TransactionId = transactionId.ToString(),
                    FailedState = (int)EventStateEnum.PublishedFailed
                });

                var logEntries = result.Select(row => new IntegrationEventLogEntry
               (Guid.Parse(row.EventId), row.EventTypeName,
               (EventStateEnum)row.State,
               row.TimesSent,
               row.CreationTime,
               row.Content,
               Guid.Parse(row.TransactionId)
               )).ToList();

                return logEntries.Any() ?
                    logEntries.Select(e => e.DeserializeJsonContent(_eventTypes.FirstOrDefault(t => t.Name == e.EventTypeShortName))) :
                    Enumerable.Empty<IntegrationEventLogEntry>();
            }
        }

        /// <summary>
        /// Retrieves a collection of integration event log entries that are pending to be published.
        /// </summary>
        /// <returns>An enumerable collection of <see cref="IntegrationEventLogEntry"/> objects.</returns>
        public async Task<IEnumerable<IntegrationEventLogEntry>> RetrieveEventLogsPendingToPublishAsync()
        {
            const string sql = @"SELECT 
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

            using (var connection = EnsureCreateAndOpenConnection())
            {
                var result = await connection.QueryAsync<IntegrationEventLogEntryEntity>(sql, new
                {
                    State = (int)EventStateEnum.NotPublished
                });

                var logEntries = result.Select(row => new IntegrationEventLogEntry
               (Guid.Parse(row.EventId), row.EventTypeName,
               (EventStateEnum)row.State,
               row.TimesSent,
               row.CreationTime,
               row.Content,
               Guid.Parse(row.TransactionId)
               )).ToList();

                return logEntries.Any() ?
                    logEntries.Select(e => e.DeserializeJsonContent(_eventTypes.FirstOrDefault(t => t.Name == e.EventTypeShortName))) :
                    Enumerable.Empty<IntegrationEventLogEntry>();
            }
        }

        /// <summary>
        /// Retrieves a collection of integration event log entries that are pending to be published and belong to the specified transaction.
        /// </summary>
        /// <returns>An enumerable collection of <see cref="IntegrationEventLogEntry"/> objects.</returns>
        public async Task<IEnumerable<IntegrationEventLogEntry>> RetrieveEventLogsPendingToPublishAsync(Guid transactionId)
        {
            const string sql = @"SELECT 
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

            using (var connection = EnsureCreateAndOpenConnection())
            {
                var result = await connection.QueryAsync<IntegrationEventLogEntryEntity>(sql, new
                {
                    TransactionId = transactionId,
                    State = (int)EventStateEnum.NotPublished
                });

                var logEntries = result.Select(row => new IntegrationEventLogEntry
               (Guid.Parse(row.EventId), row.EventTypeName,
               (EventStateEnum)row.State,
               row.TimesSent,
               row.CreationTime,
               row.Content,
               Guid.Parse(row.TransactionId)
               )).ToList();

                return logEntries.Any() ?
                    logEntries.Select(e => e.DeserializeJsonContent(_eventTypes.FirstOrDefault(t => t.Name == e.EventTypeShortName))) :
                    Enumerable.Empty<IntegrationEventLogEntry>();
            }
        }

        /// <summary>
        /// Saves the specified integration event to the event log.
        /// </summary>
        public async Task SaveEventAsync(IntegrationEvent @event)
        {
            var eventLogEntry = new IntegrationEventLogEntry(@event);

            const string sql = @"
        INSERT INTO IntegrationEventLog 
            (EventId, EventTypeName, State, TimesSent, CreationTime, Content, TransactionId)
        VALUES 
            (@EventId, @EventTypeName, @State, @TimesSent, @CreationTime, @Content, @TransactionId)";

            using (var connection = EnsureCreateAndOpenConnection())
            {
                await connection.ExecuteAsync(sql, new
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
        }

        /// <summary>
        /// Saves the specified integration event to the event log within the context of the specified transaction.
        /// </summary>
        public async Task SaveEventAsync(IntegrationEvent @event, IDbContextTransaction transaction)
        {
            ArgumentNullException.ThrowIfNull(transaction);

            var eventLogEntry = new IntegrationEventLogEntry(@event, transaction.TransactionId);

            const string sql = @"
        INSERT INTO IntegrationEventLog 
            (EventId, EventTypeName, State, TimesSent, CreationTime, Content, TransactionId)
        VALUES 
            (@EventId, @EventTypeName, @State, @TimesSent, @CreationTime, @Content, @TransactionId)";

            using (var connection = EnsureCreateAndOpenConnection())
            {
                await connection.ExecuteAsync(sql, new
                {
                    eventLogEntry.EventId,
                    eventLogEntry.EventTypeName,
                    State = (int)eventLogEntry.State,
                    eventLogEntry.TimesSent,
                    eventLogEntry.CreationTime,
                    eventLogEntry.Content,
                    eventLogEntry.TransactionId
                }, transaction.GetDbTransaction());
            }
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
            const string sql = @"
        UPDATE IntegrationEventLog
        SET 
            State = @Status,
            TimesSent = CASE WHEN @Status = @InProgressState THEN TimesSent + 1 ELSE TimesSent END
        WHERE EventId = @EventId";

            using (var connection = EnsureCreateAndOpenConnection())
            {
                await connection.ExecuteAsync(sql, new
                {
                    EventId = eventId,
                    Status = (int)status,
                    InProgressState = (int)EventStateEnum.InProgress
                });
            }
        }

        private DbConnection EnsureCreateAndOpenConnection()
        {
            if (_dbConnectionString == null)
            {
                throw new InvalidOperationException("Database connection is not initialized.");
            }

            var connection = DbConnectionFactory.CreateConnection(_dbConnectionString, _providerName);
            connection.Open();
            return connection;
        }

        private sealed class IntegrationEventLogEntryEntity()
        {
            public string EventId { get; private set; }

            [Required]
            public string EventTypeName { get; private set; }

            public EventStateEnum State { get; set; }

            public int TimesSent { get; set; }

            public DateTime CreationTime { get; private set; }

            [Required]
            public string Content { get; private set; }

            public string TransactionId { get; private set; }

        }
    }
}
