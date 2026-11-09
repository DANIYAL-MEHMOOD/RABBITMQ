
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RABBITMQ;
using System.Text;
using System.Text.Json;

public class RabbitMQConsumer
{
    private readonly RabbitMQSettings _settings;

    public RabbitMQConsumer(IOptions<RabbitMQSettings> options)
    {
        _settings = options.Value;
    }

    public async Task<JsonElement> StartAsync()
    {
        var factory = new ConnectionFactory
        {
            HostName = _settings.HostName,
            Port = _settings.Port,
            UserName = _settings.UserName,
            Password = _settings.Password
        };

        await using var connection =
            await factory.CreateConnectionAsync();

        await using var channel =
            await connection.CreateChannelAsync();

        await channel.QueueDeclareAsync(
            queue: _settings.QueueName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: null);

        var tcs = new TaskCompletionSource<JsonElement>(
            TaskCreationOptions.RunContinuationsAsynchronously);

        var consumer = new AsyncEventingBasicConsumer(channel);

        consumer.ReceivedAsync += async (sender, args) =>
        {
            try
            {
                var body = args.Body.ToArray();

                var message = Encoding.UTF8.GetString(body);

                var jsonMessage =
                    JsonSerializer.Deserialize<JsonElement>(message);

                await channel.BasicAckAsync(
                    args.DeliveryTag, false);

                tcs.TrySetResult(jsonMessage);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);

                await channel.BasicNackAsync(
                    args.DeliveryTag,
                    multiple: false,
                    requeue: false);

                tcs.TrySetException(ex);
            }
        };

        await channel.BasicConsumeAsync(
            queue: _settings.QueueName,
            autoAck: false,
            consumer: consumer);

        var completedTask = await Task.WhenAny(
            tcs.Task,
            Task.Delay(TimeSpan.FromSeconds(10)));

        if (completedTask != tcs.Task)
        {
            throw new TimeoutException(
                "No message received from RabbitMQ.");
        }

        return await tcs.Task;
    }
}
