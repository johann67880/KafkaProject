using Confluent.Kafka;
using System;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaProject
{
    internal class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Hello World!");
            Console.ReadLine();
        }

        public async Task Producer(string message)
        {
            var config = new ProducerConfig
            {
                BootstrapServers = "host1:9092,host2: 9092",
                ClientId = Dns.GetHostName()
            };

            using (var producer = new ProducerBuilder<Null, string>(config).Build())
            {
                await producer.ProduceAsync("testTopic", new Message<Null, string> { Value = message });
            }
        }

        public async Task Consumer(string topics)
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = "host1:9092,host2:9092",
                GroupId = "groupTest",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using(var builder = new ConsumerBuilder<Null, string>(config).Build())
            {
                builder.Subscribe(topics);
                var cancellationToken = new CancellationTokenSource();

                try
                {
                    while(!cancellationToken.IsCancellationRequested)
                    {
                        var consumer = builder.Consume(cancellationToken.Token);
                    }
                }
                catch(Exception ex)
                {
                    Console.WriteLine($"{ex.Message}");
                    builder.Close();
                }
            };
        }
    }
}
