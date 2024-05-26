using System.Data;
using System.Data.Common;

public static class DbConnectionFactory
{
    public static DbConnection CreateConnection(string connectionString, string providerName)
    {
        var availableProviders = DbProviderFactories.GetFactoryClasses();
        if (!availableProviders.Rows.Cast<DataRow>().Any(row => row["InvariantName"].ToString() == providerName))
        {
            throw new InvalidOperationException($"The provider '{providerName}' is not available. Please ensure it is installed and registered.");
        }

        var factory = DbProviderFactories.GetFactory(providerName);
        var connection = factory.CreateConnection();
        if (connection == null)
        {
            throw new InvalidOperationException($"Unable to create a connection for provider '{providerName}'");
        }
        connection.ConnectionString = connectionString;
        return connection;
    }
}
