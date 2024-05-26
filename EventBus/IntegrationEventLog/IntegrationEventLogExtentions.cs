using Dapper;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using System.Data;
using System.Data.Common;

namespace EventBus.IntegrationEventLog
{
    public static class IntegrationEventLogExtentions
    {
        /// <summary>
        /// Create a new table in database with name IntegrationEventLog.
        /// </summary>
        public static void AddEventLogTable(this ModelBuilder builder)
        {
            builder.Entity<IntegrationEventLogEntry>()
                .ToTable("IntegrationEventLog")
                .HasKey(e => e.EventId);
        }

        /// <summary>
        /// Configures Dapper to create a new table in the database with the name IntegrationEventLog.
        /// </summary>
        /// <param name="connection">The database connection.</param>
        public static void AddEventLogTable(this DbConnection connection, ILogger logger)
        {
            ArgumentNullException.ThrowIfNull(connection);

            const string createTableQuery = @"
            CREATE TABLE IF NOT EXISTS IntegrationEventLog (
                EventId VARCHAR(36) PRIMARY KEY,
                EventTypeName VARCHAR(255) NOT NULL,
                State INT NOT NULL,
                TimesSent INT NOT NULL,
                CreationTime TIMESTAMP NOT NULL,
                Content TEXT NOT NULL,
                TransactionId VARCHAR(36) NOT NULL
            )";

            try
            {
                if (connection.State != ConnectionState.Open)
                {
                    connection.Open();
                }

                connection.Execute(createTableQuery);
                logger.LogInformation("IntegrationEventLog table created successfully.");
            }
            catch (DbException ex)
            {
                logger.LogError(ex, "An error occurred while creating IntegrationEventLog table.");
                throw;
            }
            finally
            {
                if (connection.State == ConnectionState.Open)
                {
                    connection.Close();
                }
            }
        }
    }
}
