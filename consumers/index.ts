import "../startup/dotenv";
import logger from "./../logger/logger";
import { has } from 'lodash';
import { dlrLogsConsumer } from './dlr-logs-consumer';
import { dlrLogDetailsConsumer } from './dlr-log-details-consumer';

// Register your consumers here
const Consumers: any = {
    dlrLogsConsumer,
    dlrLogDetailsConsumer
};

function invalidConsumerName(consumerName: string): Boolean {
    return !has(Consumers, consumerName)
}

function getAvailableConsumers() {
    const consumerNames = Object.keys(Consumers);
    const numberedConsumerNames = consumerNames.map((consumer, idx) => `${idx + 1}. ${consumer}`);
    return numberedConsumerNames.join('\n');
}

function main() {
    const consumerName = process.argv[2];

    if (invalidConsumerName(consumerName))
        return logger.error(`Valid consumer name is required\n\nAvailable consumers: \n${getAvailableConsumers()}`);

    Consumers[consumerName]();
}

main();
