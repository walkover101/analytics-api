import rabbitmq, { Connection } from './services/rabbitmq-service';
import logger from "./logger/logger";

const rabbitConnection = rabbitmq();

rabbitConnection.on("connect", (connection) => {
    logger.info("Got Connection");
    startConsumer(connection);
})
rabbitConnection.on("close", () => {
    logger.info("Local Rabbit Closed");
})
rabbitConnection.on("error", (error) => {
    logger.error(error);
})
rabbitConnection.on("retry", () => {
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