namespace EventBus
{
    /// <summary>
    /// Represents options for the event bus.
    /// </summary>
    public class EventBusOptions
    {
        /// <summary>
        /// Gets or sets the name of the subscription client.
        /// </summary>
        public string SubscriptionClientName { get; set; }

        /// <summary>
        /// Gets or sets the retry count for the event bus. Default is 10.
        /// </summary>
        public int RetryCount { get; set; } = 10;
    }
}