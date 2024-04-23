using Microsoft.EntityFrameworkCore;

namespace EventBus.IntegrationEventLog;

public class ResilientTransaction
{
    private readonly DbContext _context;
    private ResilientTransaction(DbContext context) =>
        _context = context ?? throw new ArgumentNullException(nameof(context));

    /// <summary>
    /// Creates a new instance of the ResilientTransaction class with the specified database context.
    /// </summary>
    /// <param name="context">Database context.</param>
    /// <returns>A new instance of the ResilientTransaction class.</returns>
    public static ResilientTransaction New(DbContext context) => new(context);

    /// <summary>
    /// Executes the specified actions within a new database transaction.
    /// </summary>
    /// <param name="actions">The collection of actions to execute.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="ArgumentNullException">Thrown when the actions collection is null.</exception>
    public async Task ExecuteAsync(IEnumerable<Func<Task>> actions)
    {
        ArgumentNullException.ThrowIfNull(actions);

        var strategy = _context.Database.CreateExecutionStrategy();

        await strategy.ExecuteAsync(async () =>
        {
            await using var transaction = await _context.Database.BeginTransactionAsync();

            try
            {
                foreach (var action in actions)
                {
                    await action();
                }

                await transaction.CommitAsync();
            }
            catch
            {
                await transaction.RollbackAsync();
                throw;
            }
        });
    }
}
