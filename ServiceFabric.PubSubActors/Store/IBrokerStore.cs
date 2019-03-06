using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using ServiceFabric.PubSubActors.State;

namespace ServiceFabric.PubSubActors.Store
{
    public interface IBrokerStore
    {
        Task Initialize(CancellationToken cancellationToken = default(CancellationToken));

        Task AddOrUpdate(string messageTypeName, ReferenceWrapper reference);

        Task Remove(string messageTypeName, ReferenceWrapper reference);

        Task ProcessQueues(CancellationToken cancellationToken);

        Task EnqueueMessage(MessageWrapper message);

        IEnumerable<KeyValuePair<string, ReferenceWrapper>> Queues { get; }

        bool IsInitialized { get; }
    }
}
