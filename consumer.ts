import rabbitmq, { Connection } from './services/rabbitmq-service';
import logger from "./logger/logger";
const localRabbit = rabbitmq('amqps://mfdbfjvm:Vb_uzFbnTZ40f43D9cgaENNyiYsuO-vg@puffin.rmq2.cloudamqp.com/mfdbfjvm');

localRabbit.on("connect", (connection) => {
    logger.info("Got Connection");
    startConsumer(connection);
})
localRabbit.on("close", () => {
    logger.info("Local Rabbit Closed");
})
localRabbit.on("error", (error) => {
    logger.error(error);
})
localRabbit.on("retry", () => {
    logger.info("Retrying");
})

async function startConsumer(connection: Connection) {
    try {
        const channel = await connection.createChannel();
        var queue = "hello";
        channel.assertQueue(queue, {
            durable: false
        })
        channel.consume(queue, (msg: any) => {
            logger.info(" [x] Received %s", msg.content.toString());
        }, {
            noAck: true
        });

    } catch (error) {
        logger.error(error);
    }


}