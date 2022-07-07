import rabbitmq, { Connection } from './startup/rabbitmq';
import { delay } from './utility';
const localRabbit = rabbitmq("amqps://mfdbfjvm:Vb_uzFbnTZ40f43D9cgaENNyiYsuO-vg@puffin.rmq2.cloudamqp.com/mfdbfjvm");
localRabbit.on("connect", (connection) => {
    console.log("Got Connection");
    startProducer(connection);
});
localRabbit.on("error", (error) => {
    console.log(error);
})

async function startProducer(connection: Connection) {
    try {
        const channel = await connection.createChannel()
        var queue = "hello";
        channel.assertQueue(queue, {
            durable: false
        });
        let condition = true;
        channel.on("error", (error: any) => {
            condition = false;
        })
        let i = 0;
        while (condition) {
            await delay(2000);
            var msg = "Welcome World-" + (i++);
            try {

                channel.sendToQueue(queue, Buffer.from(msg));
            } catch (reason) {
                console.log("Error");
                condition = false;
            }
            console.log("[x] Sent %s", msg);
        }

    } catch (error) {
        console.log("Error Occurred");
    }
}