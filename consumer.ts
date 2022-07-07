import rabbitmq, { Connection } from './startup/rabbitmq';
const localRabbit = rabbitmq('amqps://mfdbfjvm:Vb_uzFbnTZ40f43D9cgaENNyiYsuO-vg@puffin.rmq2.cloudamqp.com/mfdbfjvm');
localRabbit.on("connect", (connection) => {
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

async function startConsumer(connection: Connection) {
    try {
        const channel = await connection.createChannel();
        var queue = "hello";
        channel.assertQueue(queue, {
            durable: false
        })
        channel.consume(queue, (msg: any) => {
            console.log(" [x] Received %s", msg.content.toString());
        }, {
            noAck: true
        });

    } catch (error) {
        console.log(error);
    }


}