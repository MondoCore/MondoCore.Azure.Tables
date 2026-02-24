# MondoCore.Azure.Tables
  Read and write to Azure tables (e.g. Azure storage tables or the Cosmos table API)
 
<br>


#### AzureTable

```
using MondoCore.Azure.Tables;

public static class Example
{
    public static async Task DoWork(string connectionString, string tableName)
    {
        // Create an instance of AzureTable with a connection string and a table name
        ITable table = new AzureTable(tableName, connectionString, partitionKeyField: "Make");

        // insert a new record into the table
        await table.Writer.Insert(new Car { Id = "123", Make = "Chevy", Model = "Corvette", Year = 1956});
    }
}
```

See [MondoCore.Data](https://github.com/MondoCore/MondoCore.Data) for full interface reference.

```
License
----

MIT
