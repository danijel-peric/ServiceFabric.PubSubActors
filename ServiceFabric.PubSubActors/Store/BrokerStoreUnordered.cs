using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceFabric.Data;
using Microsoft.ServiceFabric.Data.Collections;
using ServiceFabric.PubSubActors.Helpers;
using ServiceFabric.PubSubActors.State;

namespace ServiceFabric.PubSubActors.Store
{
    public class BrokerStoreUnordered : BrokerStoreBase
    {
        public BrokerStoreUnordered(IReliableStateManager stateManager, ILogProvider logProvider = null, string storeKey = "Queues") : base(stateManager, logProvider, storeKey)
        {
        }

        protected override async Task ProcessQueue(CancellationToken cancellationToken, ReferenceWrapper subscriber, string queueName)
        {
            var queue = await TimeoutRetryHelper.Execute((token, state) => StateManager.GetOrAddAsync<IReliableConcurrentQueue<MessageWrapper>>(queueName), cancellationToken: cancellationToken);
            long messageCount = queue.Count;

            if (messageCount == 0L) return;
            messageCount = Math.Min(messageCount, MaxDequeuesInOneIteration);

            LogProvider?.LogMessage($"Processing {messageCount} items from queue {queue.Name} for subscriber: {subscriber.Name}");

            for (long i = 0; i < messageCount; i++)
            {
                cancellationToken.ThrowIfCancellationRequested();

                await StateManager.ExecuteInTransaction(async (tx, token, state) =>
                {
                    var result = await queue.TryDequeueAsync(tx, cancellationToken);
                    if (result.HasValue)
                    {
                        await subscriber.PublishAsync(result.Value);
                    }
                }, cancellationToken: cancellationToken);
            }
        }

        protected override async Task EnqueueMessage(MessageWrapper message, Reference subscriber, ITransaction tx)
        {
            var queueResult = await StateManager.TryGetAsync<IReliableConcurrentQueue<MessageWrapper>>(subscriber.QueueName);
            if (!queueResult.HasValue) return;

            await queueResult.Value.EnqueueAsync(tx, message);
        }

        protected override Task CreateQueue(ITransaction tx, string name)
        {
            return StateManager.GetOrAddAsync<IReliableConcurrentQueue<MessageWrapper>>(tx, name);
        }
    }
}
