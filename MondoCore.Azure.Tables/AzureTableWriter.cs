using Azure;
using Azure.Data.Tables;
using MondoCore.Collections;
using MondoCore.Common;
using MondoCore.Data;
using System.Linq;
using System.Linq.Expressions;

namespace MondoCore.Azure.Tables
{
    internal class AzureTableWriter<TID, TVALUE> 
    {
        internal AzureTableWriter(TableServiceClient serviceClient, string tableName, IIdentifierStrategy<TID>? idStrategy) 
        {
        }
        
        #region IWriteRepository<TID, TVALUE>
        /*
        public Task<bool> Delete(TID id, CancellationToken cancellationToken = default)
        {
            return InternalDelete(id, default(TVALUE), cancellationToken);
        }

        private async Task<bool> InternalDelete(TID id, TVALUE? val, CancellationToken cancellationToken = default)
        {
            var partitionKey = await GetPartitionKey(id, val, cancellationToken);
            var sid          = SplitId(id, val);

            await this.Client.DeleteEntityAsync(partitionKey, sid.Id, cancellationToken: cancellationToken);

            return true;
        }

        public async Task<long> Delete(Expression<Func<TVALUE, bool>> guard, CancellationToken cancellationToken = default)
        {
            var result = InternalGet(guard, cancellationToken);
            var count = 0L;

            await Parallel.ForEachAsync(result, cancellationToken, async (val, token)=>
            {
                try
                { 
                    await this.InternalDelete(val.Id, val, cancellationToken: token);

                    Interlocked.Increment(ref count);
                }
                catch
                {
                }
            });

            return count;
        }

        public async Task<TVALUE> Insert(TVALUE item, CancellationToken cancellationToken = default)
        {
            var entity = ToEntity(item);

            await this.Client.AddEntityAsync<TableEntity>(entity!, cancellationToken);

            return item;
        }

        public async Task Insert(IEnumerable<TVALUE> items, CancellationToken cancellationToken = default)
        {
            await Parallel.ForEachAsync(items, async (val, token)=>
            {
                await Insert(val, token);
            });
        }

        public async Task<bool> Update(TVALUE item, Expression<Func<TVALUE, bool>>? guard = null, CancellationToken cancellationToken = default)
        {
            if(guard != null)
            { 
                var currentItem = await InternalGet(item.Id, cancellationToken: cancellationToken);
                var list        = (new List<TVALUE> {currentItem}) as IEnumerable<TVALUE>;
                var fnGuard     = guard.Compile();

                if(!list.Where(fnGuard).Any())
                    return false;
            }

            var partitionKey = await GetPartitionKey(item.Id, item, cancellationToken);

            try
            { 
                var entity = ToEntity(item);
                var result = await this.Client.UpdateEntityAsync<TableEntity>(entity, ETag.All, cancellationToken: cancellationToken);

                return result != null;
            }
            catch(RequestFailedException ex) when (ex.Status == (int)System.Net.HttpStatusCode.NotFound)
            {
                throw new NotFoundException();
            }
        }

        public async Task<long> Update(object properties, Expression<Func<TVALUE, bool>> query, CancellationToken cancellationToken = default)
        {
            var result = InternalGet(query, cancellationToken: cancellationToken); 
            var count = 0L;

            await Parallel.ForEachAsync(result, cancellationToken, async (val, token)=>
            {
                try
                { 
                    if(val.SetValues(properties))
                    { 
                        var entity = ToEntity(val);

                        await this.Client.UpdateEntityAsync<TableEntity>(entity, ETag.All, cancellationToken: token);

                        Interlocked.Increment(ref count);
                    }
                }
                catch
                {
                }
            });

            return count;
        }

        public async Task<long> Update(Func<TVALUE, Task<(bool Update, bool Continue)>> update, Expression<Func<TVALUE, bool>> query, CancellationToken cancellationToken = default)
        {
            var result = InternalGet(query, cancellationToken: cancellationToken); 
            var count = 0L;
            
            await Parallel.ForEachAsync(result, cancellationToken, async (val, token)=>
            {
                try
                { 
                    var result = await update(val);

                    if(result.Update)
                    { 
                         var entity = ToEntity(val);

                        await this.Client.UpdateEntityAsync<TableEntity>(entity, ETag.All, cancellationToken: token);

                        Interlocked.Increment(ref count);
                    }
                }
                catch
                {
                }
            });

            return count;
        }
        */
        #endregion
    }
}
