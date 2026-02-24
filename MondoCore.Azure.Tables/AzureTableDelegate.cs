using Azure;
using Azure.Data.Tables;
using MondoCore.Collections;
using MondoCore.Common;
using MondoCore.Data;
using MondoCore.Data.Tables;
using System.Globalization;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;

namespace MondoCore.Azure.Tables
{
    /**************************************************************************/
    /**************************************************************************/
    internal class AzureTableDelegate<T>(TableClient tableClient, string partitionKeyField = "PartitionKey") : TableDelegate<T>(partitionKeyField) where T : class, new()
    {
        #region Read

        public override Task<T> Get(string id, CancellationToken cancellationToken = default)
        {
            return Get(id, null, cancellationToken);
        }

        public override async Task<T> Get(string id, string? partitionKey, CancellationToken cancellationToken = default)
        {
            try
            { 
                // if the partitition key is null or empty, we should be able to query for the record using the id as the row key and a wildcard for the partition key. This is because some records may not have a partition key, and we want to be able to retrieve them using just the id. However, if the partition key is provided, we can use it to narrow down the search and improve performance.
                if(!string.IsNullOrWhiteSpace(partitionKey))
                { 
                    var response = await tableClient.GetEntityAsync<TableEntity>(partitionKey, id, cancellationToken: cancellationToken).ConfigureAwait(false);
            
                    if(response.Value == null)
                        throw new NotFoundException();

                    var rtnValue = new T();
                    var properties = response.Value.Keys.ToDictionary(k => k, k => response.Value[k]);

                    rtnValue.SetValues(properties); 

                    return rtnValue;
                }
                else
                { 
                    var queryResult = tableClient.QueryAsync<TableEntity>(e => e.RowKey == id, cancellationToken: cancellationToken);
                    
                    await foreach(var response in queryResult.ConfigureAwait(false))
                    {
                        var rtnValue = new T();
                        var properties = response.Keys.ToDictionary(k => k, k => response[k]);

                        rtnValue.SetValues(properties); 

                        return rtnValue;
                    }

                    throw new NotFoundException();
               }
            }
            catch(RequestFailedException ex) when (ex.Status == (int)System.Net.HttpStatusCode.NotFound)
            {
                throw new NotFoundException();
            }
        }

        public override async IAsyncEnumerable<T> Get(Expression<Func<T, bool>> query, [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            var queryResult   = tableClient.QueryAsync<TableEntity>(e => true, cancellationToken: cancellationToken);
            var compiledQuery = query.Compile();

            await foreach(var response in queryResult.ConfigureAwait(false))
            {
                var rtnValue = new T();

                var properties = response.Keys.ToDictionary(k => k, k => response[k]);
                rtnValue.SetValues(properties); 

                if(compiledQuery(rtnValue))
                    yield return rtnValue!;
            }
        }

        #endregion

        #region Write

        public override async Task<T> Insert(T item, CancellationToken cancellationToken = default)
        {
            var entity = ToEntity(item);

            await tableClient.AddEntityAsync<TableEntity>(entity!, cancellationToken);

            return item;
        }

        public override async Task<bool> Delete(string id, CancellationToken cancellationToken = default)
        {
            try
            { 
                var item = await Get(id, cancellationToken);

                return await Delete(item, cancellationToken);
            }
            catch(NotFoundException)
            {
                // If the entity is not found, we can consider it as already deleted, so we return true.
                return true;
            }
        }

        public override async Task<bool> Delete(T item, CancellationToken cancellationToken = default)
        {
            var partitionKey = item.GetValue<string>(partitionKeyField);

            try
            { 
                await tableClient.DeleteEntityAsync(partitionKey, item.GetValue<string>("Id"), cancellationToken: cancellationToken);
            }
            catch(RequestFailedException ex) when (ex.Status == (int)System.Net.HttpStatusCode.NotFound)
            {
                // If the entity is not found, we can consider it as already deleted, so we return true.
            }

            return true;
        }

        protected override async Task<bool> Update(T item, string? partitionKey, CancellationToken cancellationToken = default)
        {
            try
            { 
                var entity = ToEntity(item);
                var result = await tableClient.UpdateEntityAsync<TableEntity>(entity, ETag.All, cancellationToken: cancellationToken);

                return result != null;
            }
            catch(RequestFailedException ex) when (ex.Status == (int)System.Net.HttpStatusCode.NotFound)
            {
                throw new NotFoundException();
            }
        }

        #endregion

        #region Private

        private TableEntity ToEntity(T item)
        {
            var partitionKey = item.GetValue<string>(partitionKeyField);
            var entity       = new TableEntity(partitionKey, item.GetValue<string>("Id"));
            var props        = item.ToDictionary();

            foreach(var prop in props)
                entity[prop.Key] = prop.Value;

            return entity;
        }

        #endregion
    }
}
