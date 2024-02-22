using Confluent.Kafka;

namespace Kafka.Producer
{
    internal class Program
    {
        static async Task Main(string[] args)
        {
            var config = new ProducerConfig
            {
                BootstrapServers = "PLAINTEXT://localhost:29092",
                SecurityProtocol = SecurityProtocol.Plaintext,
            };

            using var producer = new ProducerBuilder<string, string>(config).Build();
            string topic = "test.poc";

            while (true)
            {
                Console.Write("Enter your message: ");
                var msg = Console.ReadLine();
                var message = new Message<string, string>
                {
                    Value = msg,
                    Key = msg
                };

                var result = await producer.ProduceAsync(topic, message);
              Console.WriteLine(result.Status);
            }
        }
    }
}
