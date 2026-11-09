using Microsoft.AspNetCore.Mvc;
using RabbitMQ.Client;
using System.Text;
using System.Text.Json;

namespace RABBITMQ.Controllers
{
    public class RABBITMQController : Controller
    {
        private readonly RabbitMQPublisher _rabbitMQPublisher;
        private readonly RabbitMQConsumer _rabbitMQConsumer;

        public RABBITMQController(RabbitMQPublisher rabbitMQPublisher, RabbitMQConsumer rabbitMQConsumer)
        {
            _rabbitMQPublisher = rabbitMQPublisher;
            _rabbitMQConsumer = rabbitMQConsumer;
        }


        [HttpPost("publish")]
        public async Task<IActionResult> PublishMessage(
            [FromBody] object message)
        {
            await _rabbitMQPublisher.PublishAsync(message);

            return Ok(new
            {
                Success = true,
                Message = "Message published successfully.",
                Data = message
            });
        }



        [HttpPost("Consume")]
        public async Task<IActionResult> ConsumeMessage()
        {
            try
            {
                var message = await _rabbitMQConsumer.StartAsync();

                return Ok(new
                {
                    Success = true,
                    Message = "Message consumed successfully.",
                    Data = message
                });
            }
            catch (TimeoutException ex)
            {
                return StatusCode(408, new
                {
                    Success = false,
                    Message = ex.Message
                });
            }
        }

      
    }
}
