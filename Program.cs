using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using MQTTnet;
using MQTTnet.Protocol;
using MQTTnet.Server;
using Newtonsoft.Json;
using Serilog;

namespace broker_console
{
    class Program
    {
        static async Task Main(string[] args)
        {
            Log.Logger = new LoggerConfiguration().MinimumLevel
                .Information()
                .WriteTo.Console()
                .WriteTo.File(
                    "./logs/log.txt",
                    rollingInterval: RollingInterval.Day,
                    rollOnFileSizeLimit: true
                )
                .CreateLogger();
            // Create the options for our MQTT Broker
            MqttServerOptionsBuilder options = new MqttServerOptionsBuilder()
                // log
                .WithConnectionBacklog(100)
                // set endpoint to localhost
                .WithDefaultEndpoint()
                // port used will be 1883
                .WithDefaultEndpointPort(1883)
                // handler for new connections
                .WithConnectionValidator(OnNewConnection)
                // handler for new messages
                .WithApplicationMessageInterceptor(OnNewMessage)
                .WithSubscriptionInterceptor(OnNewSubscriptions)
                .WithStorage(new RetainedMessageHandler());

            IMqttServer mqttServer = new MqttFactory().CreateMqttServer();
            // mqttServer.StartAsync(options.Bu()).GetAwaiter().GetResult();
            await mqttServer.StartAsync(options.Build());
            // keep application running until user press a key
            Console.WriteLine("Broker Started");
            Console.ReadLine();
            await mqttServer.StopAsync();
            Log.CloseAndFlush();
        }

        public static void OnNewConnection(MqttConnectionValidatorContext context)
        {
            Log.Information(
                "New connection: ClientId = {clientId}, Endpoint = {endpoint}, Username: {username}, Password: {password}",
                context.ClientId,
                context.Endpoint,
                context.Username,
                context.Password
            );
            if (context.Username != "anousone")
            {
                context.ReasonCode = MqttConnectReasonCode.BadUserNameOrPassword;
                return;
            }

            if (context.Password != "qwertyanousone")
            {
                context.ReasonCode = MqttConnectReasonCode.BadUserNameOrPassword;
                return;
            }
            context.ReasonCode = MqttConnectReasonCode.Success;
        }

        static uint MessageCounter = 0;

        public static void OnNewMessage(MqttApplicationMessageInterceptorContext context)
        {
            var payload =
                context.ApplicationMessage?.Payload == null
                    ? null
                    : Encoding.UTF8.GetString(context.ApplicationMessage?.Payload);

            MessageCounter++;

            Log.Logger.Information(
                "MessageId: {MessageCounter} - TimeStamp: {TimeStamp} -- Message: ClientId = {clientId}, Topic = {topic}, Payload = {payload}, QoS = {qos}, Retain-Flag = {retainFlag}",
                MessageCounter,
                DateTime.Now,
                context.ClientId,
                context.ApplicationMessage?.Topic,
                payload,
                context.ApplicationMessage?.QualityOfServiceLevel,
                context.ApplicationMessage?.Retain
            );

            if (context.ApplicationMessage.Topic == "wsm/sensor")
            {
                context.ApplicationMessage.Payload = Encoding.UTF8.GetBytes(
                    "DateTime: " + DateTime.Now
                );
            }

            // It is possible to disallow the sending of messages for a certain client id like this:
            if (context.ClientId == "anonymous")
            {
                context.AcceptPublish = false;
                return;
            }
            // It is also possible to read the payload and extend it. For example by adding a timestamp in a JSON document.
            // This is useful when the IoT device has no own clock and the creation time of the message might be important.
        }

        public static void OnNewSubscriptions(MqttSubscriptionInterceptorContext context)
        {
            // intercept wsm
            // if (!context.TopicFilter.Topic.StartsWith("api"))
            // {
            //     context.AcceptSubscription = false;
            // }
            // if (context.TopicFilter.Topic.StartsWith("hack") && context.ClientId == "hacker")
            // {
            //     context.AcceptSubscription = false;
            //     context.CloseConnection = true;
            // }
            // intercept api
            // if (context.TopicFilter.Topic.StartsWith("wsm") && context.ClientId == "api")
            // {
            //     context.AcceptSubscription = true;
            // }
        }
    }

    // The implementation of the storage:
    // This code uses the JSON library "Newtonsoft.Json".
    public class RetainedMessageHandler : IMqttServerStorage
    {
        // private const string Filename = "C:\\MQTT\\RetainedMessages.json";
        private const string Filename = "RetainedMessages.json";

        public Task SaveRetainedMessagesAsync(IList<MqttApplicationMessage> messages)
        {
            // Console.WriteLine("=> Decode Payload: " + Encoding.UTF8.GetString(messages[0].Payload));
            var json = JsonConvert.SerializeObject(messages);
            File.WriteAllText(Filename, json);
            return Task.FromResult(0);
        }

        public Task<IList<MqttApplicationMessage>> LoadRetainedMessagesAsync()
        {
            IList<MqttApplicationMessage> retainedMessages;
            if (File.Exists(Filename))
            {
                var json = File.ReadAllText(Filename);
                retainedMessages = JsonConvert.DeserializeObject<List<MqttApplicationMessage>>(
                    json
                );
            }
            else
            {
                retainedMessages = new List<MqttApplicationMessage>();
            }

            return Task.FromResult(retainedMessages);
        }
    }
}
