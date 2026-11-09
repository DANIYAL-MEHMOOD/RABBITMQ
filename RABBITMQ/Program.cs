using RABBITMQ;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
builder.Services.AddSingleton<RabbitMQPublisher>();
builder.Services.AddSingleton<RabbitMQConsumer>();
builder.Services.Configure<RabbitMQSettings>(
    builder.Configuration.GetSection("RABBITMQ"));


builder.Services.AddControllers();

// Automatically start RabbitMQ listener
builder.Services.AddHostedService<RabbitMQBackgroundConsumer>();

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    //app.UseSwaggerUI();
app.UseSwaggerUI();
}

app.UseHttpsRedirection();

app.UseAuthorization();

app.MapControllers();

app.Run();
