// The MIT License (MIT)
// 
// Copyright (c) 2015-2018 Rasmus Mikkelsen
// Copyright (c) 2015-2018 eBay Software Foundation
// https://github.com/eventflow/EventFlow
// 
// Permission is hereby granted, free of charge, to any person obtaining a copy of
// this software and associated documentation files (the "Software"), to deal in
// the Software without restriction, including without limitation the rights to
// use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
// the Software, and to permit persons to whom the Software is furnished to do so,
// subject to the following conditions:
// 
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
// 
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
// FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
// COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
// IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
// CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using EventFlow.Core;
using EventFlow.Extensions;
using EventFlow.Logs;
using RabbitMQ.Client;

namespace EventFlow.RabbitMQ.Integrations
{
    public class RabbitMqPublisher : IDisposable, IRabbitMqPublisher
    {
        private readonly ILog _log;
        private readonly IRabbitMqConnectionFactory _connectionFactory;
        private readonly IRabbitMqConfiguration _configuration;
        private readonly ITransientFaultHandler<IRabbitMqRetryStrategy> _transientFaultHandler;
        private readonly AsyncLock _asyncLock = new AsyncLock();
        private IRabbitConnection _connection;

        public RabbitMqPublisher(
            ILog log,
            IRabbitMqConnectionFactory connectionFactory,
            IRabbitMqConfiguration configuration,
            ITransientFaultHandler<IRabbitMqRetryStrategy> transientFaultHandler)
        {
            _log = log;
            _connectionFactory = connectionFactory;
            _configuration = configuration;
            _transientFaultHandler = transientFaultHandler;
        }

        public Task PublishAsync(CancellationToken cancellationToken, params RabbitMqMessage[] rabbitMqMessages)
        {
            return PublishAsync(rabbitMqMessages, cancellationToken);
        }

        public async Task PublishAsync(IReadOnlyCollection<RabbitMqMessage> rabbitMqMessages, CancellationToken cancellationToken)
        {
            var uri = _configuration.UriList;
            IRabbitConnection rabbitConnection = null;
            try
            {
                rabbitConnection = await GetRabbitMqConnectionAsync(uri, cancellationToken).ConfigureAwait(false);

                await _transientFaultHandler.TryAsync(
                    c => rabbitConnection.WithModelAsync(m => PublishAsync(m, rabbitMqMessages), c),
                    Label.Named("rabbitmq-publish"),
                    cancellationToken)
                    .ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception e)
            {
                if (rabbitConnection != null)
                {
                    using (await _asyncLock.WaitAsync(CancellationToken.None).ConfigureAwait(false))
                    {
                        rabbitConnection.Dispose();
                        _connection = null;
                    }
                }
                _log.Error(e, "Failed to publish domain events to RabbitMQ");
                throw;
            }
        }

        private async Task<IRabbitConnection> GetRabbitMqConnectionAsync(IEnumerable<Uri> uri, CancellationToken cancellationToken)
        {
            using (await _asyncLock.WaitAsync(cancellationToken).ConfigureAwait(false))
            {
                return _connection ?? (_connection = await _connectionFactory
                           .CreateConnectionAsync(uri, cancellationToken).ConfigureAwait(false));
            }
        }

        private Task<int> PublishAsync(
            IModel model,
            IReadOnlyCollection<RabbitMqMessage> messages)
        {
            _log.Verbose(
                "Publishing {0} domain events to RabbitMQ hosts '{1}'",
                messages.Count,
                string.Join(" ", _configuration.UriList.Select(x=>x.Host)));

            foreach (var message in messages)
            {
                var bytes = Encoding.UTF8.GetBytes(message.Message);

                var basicProperties = model.CreateBasicProperties();
                basicProperties.Headers = message.Headers.ToDictionary(kv => kv.Key, kv => (object)kv.Value);
                basicProperties.Persistent = _configuration.Persistent;
                basicProperties.Timestamp = new AmqpTimestamp(DateTimeOffset.Now.ToUnixTime());
                basicProperties.ContentEncoding = "utf-8";
                basicProperties.ContentType = "application/json";
                basicProperties.MessageId = message.MessageId.Value;

                // TODO: Evil or not evil? Do a Task.Run here?
                model.BasicPublish(message.Exchange.Value, message.RoutingKey.Value, false, basicProperties, bytes);
            }

            return Task.FromResult(0);
        }

        public void Dispose()
        {
            _connection?.Dispose();
            _connection = null;
        }
    }
}