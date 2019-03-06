using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceFabric.Data;
using Microsoft.ServiceFabric.Data.Collections;
using ServiceFabric.PubSubActors.Helpers;
using ServiceFabric.PubSubActors.State;

namespace ServiceFabric.PubSubActors.Store
{
    public class BrokerStore : BrokerStoreBase
    {
        public BrokerStore(IReliableStateManager stateManager, ILogProvider logProvider = null, string storeKey = "Queues") : base(stateManager, logProvider, storeKey)
        {
        }

        protected override Task CreateQueue(ITransaction tx, string name)
        {
            return StateManager.GetOrAddAsync<IReliableQueue<MessageWrapper>>(tx, name);
        }

        /// <summary>
        /// Takes a published message and forwards it (indirectly) to all Subscribers.
        /// </summary>
        /// <param name="message">The message to publish</param>
        /// <returns></returns>
        protected override async Task EnqueueMessage(MessageWrapper message, Reference subscriber, ITransaction tx)
        {
            var queueResult = await StateManager.TryGetAsync<IReliableQueue<MessageWrapper>>(subscriber.QueueName);
            if (!queueResult.HasValue) return;

            await queueResult.Value.EnqueueAsync(tx, message);
        }

        protected override async Task ProcessQueue(CancellationToken cancellationToken, ReferenceWrapper subscriber, string queueName)
        {
            var queue = await TimeoutRetryHelper.Execute((token, state) => StateManager.GetOrAddAsync<IReliableQueue<MessageWrapper>>(queueName), cancellationToken: cancellationToken);
            long messageCount = await StateManager.ExecuteInTransaction((tx, token, state) => queue.GetCountAsync(tx), cancellationToken: cancellationToken);

            if (messageCount == 0L) return;
            messageCount = Math.Min(messageCount, MaxDequeuesInOneIteration);

            LogProvider?.LogMessage($"Processing {messageCount} items from queue {queue.Name} for subscriber: {subscriber.Name}");

            for (long i = 0; i < messageCount; i++)
            {
                cancellationToken.ThrowIfCancellationRequested();

                await StateManager.ExecuteInTransaction(async (tx, token, state) =>
                {
                    var message = await queue.TryDequeueAsync(tx);
                    if (message.HasValue)
                    {
                        await subscriber.PublishAsync(message.Value);
                    }
                }, cancellationToken: cancellationToken);
            }
        }
    }
}
