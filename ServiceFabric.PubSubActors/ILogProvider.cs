using System.Runtime.CompilerServices;

namespace ServiceFabric.PubSubActors
{
    public interface ILogProvider
    {
        void LogMessage(string message, [CallerMemberName] string caller = "unknown");
    }
}
