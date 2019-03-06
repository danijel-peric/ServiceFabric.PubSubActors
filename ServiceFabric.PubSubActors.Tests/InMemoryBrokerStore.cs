using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using ServiceFabric.PubSubActors.State;
using ServiceFabric.PubSubActors.Store;

namespace ServiceFabric.PubSubActors.Tests
{
    public class InMemoryBrokerStore : IBrokerStore
    {
        private readonly string _storeKey;
        private readonly ConcurrentDictionary<string, ReferenceWrapper> _queues;
        private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, BrokerServiceState>> _state;

        public InMemoryBrokerStore(string storeKey)
        {
            _storeKey = storeKey;
            _queues = new ConcurrentDictionary<string, ReferenceWrapper>();
            _state = new ConcurrentDictionary<string, ConcurrentDictionary<string, BrokerServiceState>>();
        }

        public Task Initialize(CancellationToken cancellationToken = default(CancellationToken))
        {
            return Task.CompletedTask;
        }

        public Task AddOrUpdate(string messageTypeName, ReferenceWrapper reference)
        {
            var myDictionary = _state.GetOrAdd(messageTypeName, s => new ConcurrentDictionary<string, BrokerServiceState>());

            var queueName = CreateQueueName(messageTypeName, reference);

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

            myDictionary.AddOrUpdate(_storeKey, AddValueFactory, UpdateValueFactory);

            _queues.AddOrUpdate(queueName, reference, (key, old) => reference);

            return Task.CompletedTask;
        }

        public Task Remove(string messageTypeName, ReferenceWrapper reference)
        {
            var myDictionary = _state.GetOrAdd(messageTypeName, s => new ConcurrentDictionary<string, BrokerServiceState>());
            
            var queueName = CreateQueueName(messageTypeName, reference);

            if(myDictionary.TryGetValue(_storeKey, out BrokerServiceState subscribers))
            {
                var newState = BrokerServiceState.RemoveSubscriber(subscribers, reference);
                myDictionary.AddOrUpdate(_storeKey, newState, (s, state) => newState);
            }
         
            _queues.TryRemove(queueName, out reference);

            return Task.CompletedTask;
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

        private Task ProcessQueue(CancellationToken cancellationToken, ReferenceWrapper subscriber, string queueName)
        {
            return Task.CompletedTask;
        }

        public async Task EnqueueMessage(MessageWrapper message)
        {
            var myDictionary = _state.GetOrAdd(message.MessageType, s => new ConcurrentDictionary<string, BrokerServiceState>());

            myDictionary.TryGetValue(_storeKey, out BrokerServiceState found);

            var subscribers = found?.Subscribers.ToArray();

            if (subscribers == null || subscribers.Length == 0)
                return;

            foreach (var subscriber in subscribers)
            {
                await EnqueueMessage(message, subscriber);
            }
        }


        private Task EnqueueMessage(MessageWrapper message, Reference subscriber)
        {
            return Task.CompletedTask;
        }

        public IEnumerable<KeyValuePair<string, ReferenceWrapper>> Queues => _queues;

        public bool IsInitialized => true;

        private static string CreateQueueName(string messageTypeName, ReferenceWrapper reference)
        {
            return $"{messageTypeName}_{reference.GetQueueName()}";
        }
    }
}
