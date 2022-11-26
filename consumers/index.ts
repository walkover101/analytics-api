import "../startup/dotenv";
import '../startup/string.extensions';
import logger from "./../logger/logger";
import { has } from 'lodash';
import { mailRequestsConsumer } from './mail-requests-consumer';
import { mailReportsConsumer } from './mail-reports-consumer';
import { mailEventsConsumer } from './mail-events-consumer';
import { waReportsConsumer } from "./wa-reports-consumer";
import { waRequestsConsumer } from "./wa-requests-consumer";
import { zipFolderConsumer } from "./zip-folder-consumer";
import { notificationConsumer } from "./notification-consumer";
import { smsRTSync } from "./sms-rt-sync";
import { requestSync } from "../jobs/request-sync";

// Register your consumers here
const Consumers: any = {
    mailRequestsConsumer,
    mailReportsConsumer,
    mailEventsConsumer,
    waReportsConsumer,
    waRequestsConsumer,
    zipFolderConsumer,
    notificationConsumer,
    smsRTSync
};

function invalidConsumerName(consumerName: string): Boolean {
    return !has(Consumers, consumerName)
}

function getAvailableConsumers() {
    const consumerNames = Object.keys(Consumers);
    const numberedConsumerNames = consumerNames.map((consumer, idx) => `${idx + 1}. ${consumer}`);
    return numberedConsumerNames.join('\n');
}

async function main() {
    const consumerName = process.argv[2];

    if (invalidConsumerName(consumerName))
        return logger.error(`Valid consumer name is required\n\nAvailable consumers: \n${getAvailableConsumers()}`);

    await Consumers[consumerName]();
}

main();
