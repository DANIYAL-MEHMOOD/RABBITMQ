
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RABBITMQ;
using System.Text;
using System.Text.Json;

public class RabbitMQBackgroundConsumer : BackgroundService
{
    private readonly RabbitMQSettings _settings;
    private readonly ILogger<RabbitMQBackgroundConsumer> _logger;

    public RabbitMQBackgroundConsumer(
        IOptions<RabbitMQSettings> options,
        ILogger<RabbitMQBackgroundConsumer> logger)
    {
        _settings = options.Value;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(
        CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var factory = new ConnectionFactory
                {
                    HostName = _settings.HostName,
                    Port = _settings.Port,
                    UserName = _settings.UserName,
                    Password = _settings.Password
                };

                await using var connection =
                    await factory.CreateConnectionAsync(stoppingToken);

                await using var channel =
                    await connection.CreateChannelAsync(
                        cancellationToken: stoppingToken);

                await channel.QueueDeclareAsync(
                    queue: _settings.QueueName,
                    durable: true,
                    exclusive: false,
                    autoDelete: false,
                    arguments: null,
                    cancellationToken: stoppingToken);

                // Process one message at a time
                await channel.BasicQosAsync(
                    0, 1, false, stoppingToken);

                var consumer =
                    new AsyncEventingBasicConsumer(channel);

                consumer.ReceivedAsync += async (sender, args) =>
                {
                    try
                    {
                        var body = args.Body.ToArray();

                        var json = Encoding.UTF8.GetString(body);

                        var message =
                            JsonSerializer.Deserialize<JsonElement>(json);

                        _logger.LogInformation(
                            "RabbitMQ Message Received: {Message}", json);

                        // Process your business logic here
                        await ProcessMessageAsync(message);

                        // ACK only after successful processing
                        await channel.BasicAckAsync(
                            args.DeliveryTag,
                            false,
                            stoppingToken);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex,
                            "RabbitMQ message processing failed.");

                        // Requeue failed message for retry.
                        await channel.BasicNackAsync(
                            args.DeliveryTag,
                            false,
                            true,
                            stoppingToken);
                    }
                };

                await channel.BasicConsumeAsync(
                    queue: _settings.QueueName,
                    autoAck: false,
                    consumer: consumer,
                    cancellationToken: stoppingToken);

                _logger.LogInformation(
                    "RabbitMQ Listener Started. Queue: {Queue}",
                    _settings.QueueName);

                // Keep listening until application stops
                await Task.Delay(
                    Timeout.Infinite,
                    stoppingToken);
            }
            catch (OperationCanceledException)
                when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex,
                    "RabbitMQ connection failed. Reconnecting in 5 seconds.");

                await Task.Delay(
                    TimeSpan.FromSeconds(5),
                    stoppingToken);
            }
        }
    }

    private async Task ProcessMessageAsync(JsonElement message)
    {
        // Implement your database/business logic here.

        _logger.LogInformation(
            "Processing RabbitMQ message: {Message}",
            message.ToString());

        await Task.CompletedTask;
    }
}
