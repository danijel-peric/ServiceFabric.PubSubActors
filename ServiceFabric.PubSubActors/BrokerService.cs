using System;
using System.Collections.Generic;
using System.Fabric;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceFabric.Data;
using Microsoft.ServiceFabric.Services.Communication.Runtime;
using Microsoft.ServiceFabric.Services.Remoting.V2.FabricTransport.Runtime;
using Microsoft.ServiceFabric.Services.Runtime;
using ServiceFabric.PubSubActors.Helpers;
using ServiceFabric.PubSubActors.State;
using ServiceFabric.PubSubActors.Store;
using ServiceFabric.PubSubActors.Subscriber;

namespace ServiceFabric.PubSubActors
{
    /// <remarks>
    /// Base class for a <see cref="StatefulService"/> that serves as a Broker that accepts messages from Actors &
    /// Services and forwards them to <see cref="ISubscriberActor"/> Actors and <see cref="ISubscriberService"/>
    /// Services.  Every message type is mapped to one of the partitions of this service.
    /// </remarks>
    public class BrokerService : StatefulService, ILogProvider, IBrokerService
    {
        private readonly IBrokerStore _store;

        /// <summary>
        /// The name that the <see cref="ServiceReplicaListener"/> instance will get.
        /// </summary>
        public const string ListenerName = BrokerServiceListenerSettings.ListenerName;

        /// <summary>
        /// When Set, this callback will be used to trace Service messages to.
        /// </summary>
        protected Action<string> ServiceEventSourceMessageCallback { get; set; }

        /// <summary>
        /// Gets or sets the interval to wait before starting to publish messages. (Default: 5s after Activation)
        /// </summary>
        protected TimeSpan DueTime { get; set; } = TimeSpan.FromSeconds(5);

        /// <summary>
        /// Gets or sets the interval to wait between batches of publishing messages. (Default: 5s)
        /// </summary>
        protected TimeSpan Period { get; set; } = TimeSpan.FromSeconds(5);

        /// <summary>
        /// Get or Sets the maximum period to process messages before allowing enqueuing
        /// </summary>
        protected TimeSpan MaxProcessingPeriod { get; set; } = TimeSpan.FromSeconds(3);

     

        /// <summary>
        /// Creates a new instance using the provided context and registers this instance for automatic discovery if needed.
        /// </summary>
        /// <param name="serviceContext"></param>
        /// <param name="store"></param>
        /// <param name="enableAutoDiscovery"></param>
        protected BrokerService(StatefulServiceContext serviceContext, IBrokerStore store, bool enableAutoDiscovery = true)
            : base(serviceContext)
        {
            _store = store;

            if (enableAutoDiscovery)
            {
                new BrokerServiceLocator().RegisterAsync(Context.ServiceName)
                    .ConfigureAwait(false)
                    .GetAwaiter()
                    .GetResult();
            }
        }

        /// <summary>
        /// Creates a new instance using the provided context and registers this instance for automatic discovery if needed.
        /// </summary>
        /// <param name="serviceContext"></param>
        /// <param name="reliableStateManagerReplica"></param>
        /// <param name="store"></param>
        /// <param name="enableAutoDiscovery"></param>
        protected BrokerService(StatefulServiceContext serviceContext, 
            IReliableStateManagerReplica2 reliableStateManagerReplica, IBrokerStore store, bool enableAutoDiscovery = true)
            : base(serviceContext, reliableStateManagerReplica)
        {
            _store = store;

            if (enableAutoDiscovery)
            {
                new BrokerServiceLocator().RegisterAsync(Context.ServiceName)
                    .ConfigureAwait(false)
                    .GetAwaiter()
                    .GetResult();
            }
        }

        /// <summary>
        /// Registers a Service or Actor <paramref name="reference"/> as subscriber for messages of type <paramref name="messageTypeName"/>
        /// </summary>
        /// <param name="reference">Reference to the Service or Actor to register.</param>
        /// <param name="messageTypeName">Full type name of message object.</param>
        /// <returns></returns>
        public async Task SubscribeAsync(ReferenceWrapper reference, string messageTypeName)
        {
            await _store.AddOrUpdate(messageTypeName, reference);

            ServiceEventSourceMessage($"Registered subscriber: {reference.Name}");
        }

        /// <summary>
        /// Unregisters a Service or Actor <paramref name="reference"/> as subscriber for messages of type <paramref name="messageTypeName"/>
        /// </summary>
        /// <param name="reference"></param>
        /// <param name="messageTypeName"></param>
        /// <returns></returns>
        public async Task UnsubscribeAsync(ReferenceWrapper reference, string messageTypeName)
        {
            await _store.Remove(messageTypeName, reference);

            ServiceEventSourceMessage($"Unregistered subscriber: {reference.Name}");
        }

        /// <summary>
        /// Takes a published message and forwards it (indirectly) to all Subscribers.
        /// </summary>
        /// <param name="message">The message to publish</param>
        /// <returns></returns>
        public async Task PublishMessageAsync(MessageWrapper message)
        {
            await _store.EnqueueMessage(message);
        }

        /// <summary>
        /// Starts a loop that processes all queued messages.
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        protected override async Task RunAsync(CancellationToken cancellationToken)
        {
            await _store.Initialize(cancellationToken);

            ServiceEventSourceMessage($"Sleeping for {DueTime.TotalMilliseconds}ms before starting to publish messages.");

            await Task.Delay(DueTime, cancellationToken);

            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();

                //process messages for given time, then allow other transactions to enqueue messages
                var cts = new CancellationTokenSource(MaxProcessingPeriod);
                var linkedTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cts.Token, cancellationToken);
                try
                {
                    await _store.ProcessQueues(linkedTokenSource.Token);
                }
                catch (TaskCanceledException)
                {//swallow and move on..
                }
                catch (OperationCanceledException)
                {//swallow and move on..
                }
                catch (ObjectDisposedException)
                {//swallow and move on..
                }
                catch (Exception ex)
                {
                    ServiceEventSourceMessage($"Exception caught while processing messages:'{ex.Message}'");
                    //swallow and move on..
                }
                finally
                {
                    linkedTokenSource.Dispose();
                }
                await Task.Delay(Period, cancellationToken);
            }
            // ReSharper disable once FunctionNeverReturns
        }

        /// <inheritdoc />
        protected override IEnumerable<ServiceReplicaListener> CreateServiceReplicaListeners()
        {
            //add the pubsub listener
            yield return new ServiceReplicaListener(context => new FabricTransportServiceRemotingListener(context, this), ListenerName);
        }

        /// <summary>
        /// Outputs the provided message to the <see cref="ServiceEventSourceMessageCallback"/> if it's configured.
        /// </summary>
        /// <param name="message"></param>
        /// <param name="caller"></param>
        protected void ServiceEventSourceMessage(string message, [CallerMemberName] string caller = "unknown")
        {
            ServiceEventSourceMessageCallback?.Invoke($"{caller} - {message}");
        }

        public void LogMessage(string message, [CallerMemberName] string caller = "unknown")
        {
            ServiceEventSourceMessage(message, caller);
        }
    }
}