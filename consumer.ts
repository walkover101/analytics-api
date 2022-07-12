import rabbitmq, { Connection } from './services/rabbitmq-service';
import logger from "./logger/logger";

const rabbitConnection = rabbitmq();
const BATCH_SIZE = 50;

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
        const batch: any[] = [];
        const channel = await connection.createChannel();
        var queue = "hello";

        channel.assertQueue(queue, {
            durable: false
        });

        channel.consume(queue, async (msg: any) => {
            logger.info(" [x] Received %s", msg.content.toString());
            batch.push(msg.content.toString());

            if (batch.length >= BATCH_SIZE) {
                await processMsgs(batch);
                channel.ack(msg, true); // true: Multiple Ack
            }
        }, { noAck: false }); // Auto Ack Off
    } catch (error) {
        logger.error(error);
    }
}

function processMsgs(msgs: any[]) {
    return Promise.resolve();
}