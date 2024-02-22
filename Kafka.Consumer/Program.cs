using Confluent.Kafka;

namespace Kafka.Consumer
{
    internal class Program
    {
        static void Main(string[] args)
        {
            var conf = new ConsumerConfig
            {
                BootstrapServers = "PLAINTEXT://localhost:29092",
                SecurityProtocol = SecurityProtocol.Plaintext,
                GroupId = "Demo_consumer_"+Guid.NewGuid(),
                ClientId = "Demo_client",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false
            };

            using var c = new ConsumerBuilder<string, string>(conf).Build();
            c.Subscribe("test.poc");

            CancellationTokenSource cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true;
                cts.Cancel();
            };
            while (true)
            {
                var cr = c.Consume(cts.Token);
                try
                {
                    try
                    {
                        Console.WriteLine($"Consumed message '{cr.Message.Value}' at: '{cr.TopicPartitionOffset}'.");
                        if (cr.Message.Value == "error")
                        {
                            throw new ArgumentException("Some intentional error");
                        }

                        c.Commit(cr);
                    }
                    catch (ConsumeException e)
                    {
                        Console.WriteLine($"Error occurred: {e.Error.Reason}");
                    }

                }
                catch (Exception ex)
                {
                    Console.WriteLine("Error occurred: " + ex);
                }

            }
        }
    }
}