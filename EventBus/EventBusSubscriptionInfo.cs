using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace EventBus
{
    /// <summary>
    /// Represents subscription information for the event bus.
    /// </summary>
    public class EventBusSubscriptionInfo
    {
        /// <summary>
        /// A dictionary that holds the event types and their corresponding Type objects
        /// </summary>
        public Dictionary<string, Type> EventTypes { get; } = [];

        /// <summary>
        /// Gets the options used for JSON serialization.
        /// </summary>
        public JsonSerializerOptions JsonSerializerOptions { get; } = new(DefaultSerializerOptions);

        /// <summary>
        /// The default options for JSON serialization.
        /// </summary>
        internal static readonly JsonSerializerOptions DefaultSerializerOptions = new()
        {
            TypeInfoResolver = JsonSerializer.IsReflectionEnabledByDefault ? CreateDefaultTypeResolver() : JsonTypeInfoResolver.Combine()
        };

        private static IJsonTypeInfoResolver CreateDefaultTypeResolver()
            => new DefaultJsonTypeInfoResolver();
    }
}
