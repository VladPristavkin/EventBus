using Microsoft.EntityFrameworkCore;

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
    }
}
