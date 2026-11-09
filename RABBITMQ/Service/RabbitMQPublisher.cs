using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RABBITMQ;
using System.Text;
using System.Text.Json;

public class RabbitMQPublisher
{
    private readonly RabbitMQSettings _settings;

    public RabbitMQPublisher(
        IOptions<RabbitMQSettings> options)
    {
        _settings = options.Value;
    }

    public async Task PublishAsync<T>(T message)
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

        var json = JsonSerializer.Serialize(message);
        var body = Encoding.UTF8.GetBytes(json);

        var properties = new BasicProperties
        {
            Persistent = true,
            ContentType = "application/json"
        };

        await channel.BasicPublishAsync(
            exchange: "",
            routingKey: _settings.QueueName,
            mandatory: true,
            basicProperties: properties,
            body: body);
    }
}