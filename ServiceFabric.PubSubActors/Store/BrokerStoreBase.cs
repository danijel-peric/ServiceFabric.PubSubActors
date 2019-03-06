using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceFabric.Data;
using Microsoft.ServiceFabric.Data.Collections;
using ServiceFabric.PubSubActors.Helpers;
using ServiceFabric.PubSubActors.State;

namespace ServiceFabric.PubSubActors.Store
{
    public abstract class BrokerStoreBase : IBrokerStore
    {
        /// <summary>
        /// Gets or Sets the maximum number of messages to de-queue in one iteration of process queue
        /// </summary>
        protected long MaxDequeuesInOneIteration { get; set; } = 100;

        private readonly ConcurrentDictionary<string, ReferenceWrapper> _queues;
        private readonly ManualResetEventSlim _initializer;
        private readonly SemaphoreSlim _semaphore;
        private readonly string _storeKey;

        protected BrokerStoreBase(IReliableStateManager stateManager, ILogProvider logProvider = null, string storeKey = "Queues")
        {
            _storeKey = storeKey;
            _queues = new ConcurrentDictionary<string, ReferenceWrapper>(StringComparer.OrdinalIgnoreCase);
            _initializer = new ManualResetEventSlim(false);
            _semaphore = new SemaphoreSlim(1);

            StateManager = stateManager;
            LogProvider = logProvider;
        }

        
        /// <summary>
        /// Loads all registered message queues from state and keeps them in memory. Avoids some locks in the statemanager.
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task Initialize(CancellationToken cancellationToken = default(CancellationToken))
        {
            if (IsInitialized) return;
            await Task.Run(() => InitializeAsyncInternal(cancellationToken), cancellationToken);
            _initializer.Wait(cancellationToken);
        }

        protected virtual async Task InitializeAsyncInternal(CancellationToken cancellationToken = default(CancellationToken))
        {
            if (IsInitialized) 
                return;

            try
            {
                _semaphore.Wait(cancellationToken);

                if (IsInitialized) 
                    return;

                await StateManager.ExecuteInTransaction(async (tx, token, state) =>
                {
                    _queues.Clear();

                    var enumerator = StateManager.GetAsyncEnumerator();

                    while (await enumerator.MoveNextAsync(cancellationToken))
                    {
                        var current = enumerator.Current as IReliableDictionary<string, BrokerServiceState>;

                        if (current == null) 
                            continue;

                        var result = await current.TryGetValueAsync(tx, _storeKey);

                        if (!result.HasValue) 
                            continue;

                        var subscribers = result.Value.Subscribers.ToList();

                        foreach (var subscriber in subscribers)
                        {
                            _queues.TryAdd(subscriber.QueueName, subscriber.ServiceOrActorReference);
                        }
                    }
                }, cancellationToken: cancellationToken);
               
                _initializer.Set();
            }
            finally
            {
                _semaphore.Release();
            }
        }

        public async Task AddOrUpdate(string messageTypeName, ReferenceWrapper reference)
        {
            await Initialize(CancellationToken.None);

            var myDictionary = await GetStateFor(messageTypeName);

            await StateManager.ExecuteInTransaction(async (tx, token, state) =>
            {
                var queueName = CreateQueueName(reference, messageTypeName);

                BrokerServiceState AddValueFactory(string key)
                {
                    var newState = new BrokerServiceState(messageTypeName);
                    var subscriber = new Reference(reference, queueName);
                    newState = BrokerServiceState.AddSubscriber(newState, subscriber);
                    return newState;
                }

                BrokerServiceState UpdateValueFactory(string key, BrokerServiceState current)
                {
                    var subscriber = new Reference(reference, queueName);
                    var newState = BrokerServiceState.AddSubscriber(current, subscriber);
                    return newState;
                }

                await myDictionary.AddOrUpdateAsync(tx, _storeKey, AddValueFactory, UpdateValueFactory);

                await CreateQueue(tx, queueName);

                _queues.AddOrUpdate(queueName, reference, (key, old) => reference);

            }, cancellationToken: CancellationToken.None);
        }

        public async Task Remove(string messageTypeName, ReferenceWrapper reference)
        {
            await Initialize(CancellationToken.None);

            var myDictionary = await GetStateFor(messageTypeName);
            
            var queueName = CreateQueueName(reference, messageTypeName);

            await StateManager.ExecuteInTransaction(async (tx, token, state) =>
            {
                var subscribers = await myDictionary.TryGetValueAsync(tx, _storeKey, LockMode.Update);
                if (subscribers.HasValue)
                {
                    var newState = BrokerServiceState.RemoveSubscriber(subscribers.Value, reference);
                    await myDictionary.SetAsync(tx, _storeKey, newState);
                }

                await StateManager.RemoveAsync(tx, queueName);

                _queues.TryRemove(queueName, out reference);
            });
        }

        public async Task ProcessQueues(CancellationToken cancellationToken)
        {
            var elements = Queues.ToArray();

            var tasks = new List<Task>(elements.Length);

            foreach (var element in elements)
            {
                var subscriber = element.Value;
                string queueName = element.Key;
                tasks.Add(ProcessQueue(cancellationToken, subscriber, queueName));
            }

            await Task.WhenAll(tasks);
        }

        public async Task EnqueueMessage(MessageWrapper message)
        {
            await Initialize(CancellationToken.None);

            var myDictionary = await GetStateFor(message.MessageType);

            var subscribers = await StateManager.ExecuteInTransaction(async (tx, token, state) =>
            {
                var result = await myDictionary.TryGetValueAsync(tx, _storeKey);

                return result.HasValue ? result.Value.Subscribers.ToArray() : null;
            });

            if (subscribers == null || subscribers.Length == 0) return;

            LogProvider?.LogMessage($"Publishing message '{message.MessageType}' to {subscribers.Length} subscribers.");

            await StateManager.ExecuteInTransaction(async (tx, token, state) =>
            {
                foreach (var subscriber in subscribers)
                {
                    await EnqueueMessage(message, subscriber, tx);
                }
                
                LogProvider?.LogMessage($"Published message '{message.MessageType}' to {subscribers.Length} subscribers.");
            });
        }

        private static string CreateQueueName(ReferenceWrapper reference, string messageTypeName)
        {
            return $"{messageTypeName}_{reference.GetQueueName()}";
        }

        /// <summary>
        /// Sends out queued messages for the provided queue.
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <param name="subscriber"></param>
        /// <param name="queueName"></param>
        /// <returns></returns>
        protected abstract Task ProcessQueue(CancellationToken cancellationToken, ReferenceWrapper subscriber, string queueName);

        protected abstract Task EnqueueMessage(MessageWrapper message, Reference subscriber, ITransaction tx);

        protected abstract Task CreateQueue(ITransaction tx, string name);

        Task<IReliableDictionary<string, BrokerServiceState>> GetStateFor(string messageTypeName)
        {
           return TimeoutRetryHelper.Execute((token, state) => StateManager.GetOrAddAsync<IReliableDictionary<string, BrokerServiceState>>(messageTypeName));
        }
   
        public IEnumerable<KeyValuePair<string, ReferenceWrapper>> Queues => _queues;

        protected IReliableStateManager StateManager { get; }

        protected ILogProvider LogProvider { get; }

        public bool IsInitialized => _initializer.IsSet;
    }
}
