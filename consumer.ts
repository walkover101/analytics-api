import rabbitmq, { Connection } from './startup/rabbitmq';
const localRabbit = rabbitmq('amqp://localhost');
localRabbit.on("connection", (connection) => {
    console.log("Got Connection");
    startConsumer(connection);
})
localRabbit.on("close", () => {
    console.log("Local Rabbit Closed");
})
localRabbit.on("error", (error) => {
    console.error(error);
})
localRabbit.on("retry", () => {
    console.log("Retrying");
})

function startConsumer(connection: Connection) {
    try {
        connection.createChannel((error: any, channel: any) => {
            if (error) {
                throw error;
            }
            var queue = "hello";
            channel.assertQueue(queue, {
                durable: false
            })
            channel.consume(queue, (msg: any) => {
                console.log(" [x] Received %s", msg.content.toString());
            }, {
                noAck: true
            });
        })
    } catch (error) {
        console.log(error);
    }


}