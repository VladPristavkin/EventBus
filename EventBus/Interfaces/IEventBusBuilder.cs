using Microsoft.Extensions.DependencyInjection;

namespace EventBus.Interfaces
{
    public interface IEventBusBuilder
    {
        public IServiceCollection Services { get; }
    }
}
