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

namespace EventFlow.RabbitMQ
{
    public class RabbitMqConfiguration : IRabbitMqConfiguration
    {
        public IEnumerable<Uri> UriList { get; }
        public string UserName { get; }
        public string Password { get; }
        public string VHost { get; }
        public bool Persistent { get; }
        public int ModelsPrConnection { get; }
        public string Exchange { get; }

        public static IRabbitMqConfiguration With(
            IEnumerable<Uri> uriList,
            string userName,
            string password,
            string vhost,
            bool persistent = true,
            int modelsPrConnection = 5,
            string exchange = "eventflow")
        {
            return new RabbitMqConfiguration(uriList, userName, password, vhost, persistent, modelsPrConnection, exchange);
        }

        private RabbitMqConfiguration(IEnumerable<Uri> uriList, string userName, string password, string vhost, bool persistent, int modelsPrConnection, string exchange)
        {
            if (uriList == null) throw new ArgumentNullException(nameof(uriList));
            if (string.IsNullOrEmpty(exchange)) throw new ArgumentNullException(nameof(exchange));

            UriList = uriList;
            UserName = userName;
            Password = password;
            VHost = vhost;
            Persistent = persistent;
            ModelsPrConnection = modelsPrConnection;
            Exchange = exchange;
        }
    }
}