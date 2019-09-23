using System;
using System.Collections.Generic;
using System.Linq;
using RabbitMQ.Client;

namespace EventFlow.RabbitMQ.Integrations
{
    public class ClusterEndpointResolver : IEndpointResolver
    {
        private readonly IEnumerable<Uri> _uriList;

        public ClusterEndpointResolver(IEnumerable<Uri> uriList)
        {
            _uriList = uriList;
        }
        public IEnumerable<AmqpTcpEndpoint> All()
        {
            var orderedList = _uriList.Select(rabbitUrl => new AmqpTcpEndpoint(rabbitUrl.Host, rabbitUrl.Port)).ToList();
            return orderedList.OrderBy(x => Guid.NewGuid()); //very simple way to shuffle the list around (no randomness guaranteed)
        }
    }
}
