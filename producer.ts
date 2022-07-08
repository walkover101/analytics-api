import rabbitmq, { Connection } from './services/rabbitmq-service';
import logger from "./logger/logger";
import { delay } from './utility';

const rabbitConnection = rabbitmq();

rabbitConnection.on("connect", (connection) => {
    logger.info("Got Connection");
    startProducer(connection);
});

rabbitConnection.on("error", (error) => {
    logger.error(error);
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
                logger.error("Error");
                condition = false;
            }
            logger.info("[x] Sent %s", msg);
        }

    } catch (error) {
        logger.error("Error Occurred");
    }
}