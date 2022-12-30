import rabbitmqService, { Connection, Channel } from '../database/rabbitmq-service';
import logger from "../logger/logger";
import VoiceReport from '../models/voice-report.model';
import { IConsumer } from './consumer';
import { dirname } from 'path';
let protobuf = require("protobufjs");

const appDir = dirname(require.main?.filename || '');
const voiceReport_proto = `${appDir}/models/protofiles/voice_report.proto`;
const BUFFER_SIZE = parseInt(process.env.RABBIT_VOICE_REP_BUFFER_SIZE || '1');
const QUEUE_NAME = process.env.RABBIT_VOICE_REP_QUEUE_NAME || 'voice-reports';
let batch: Array<VoiceReport> = [];


async function processMsgs(message: any, channel: Channel) {
    try {
        // let event = message?.content;
        // event = JSON.parse(event.toString());
        // protobuf.load(voiceReport_proto, async function (err: any, root: { lookupType: (arg0: string) => any; }) {
        //     if (err) throw err;
        //     const voiceReport = root.lookupType("voiceReport.voice_report");

        //     if (Array.isArray(event)) {
        //         event.forEach(e => {
        //             let message = voiceReport.create(new VoiceReport(e));
        //             let buffer = voiceReport.encode(message).finish();
        //             batch.push(buffer)
        //         });
        //     }
        //     if (batch.length >= BUFFER_SIZE) {
        //         await VoiceReport.insertMany(batch);
        //         batch = [];
        //         channel.ack(message, true);
        //     }
        // });
        let event = message?.content;
        event = JSON.parse(event.toString());
        if (Array.isArray(event)) {
            event.forEach(e => batch.push(new VoiceReport(e)));
        } else {
            batch.push(new VoiceReport(event));
        }
        if (batch.length >= BUFFER_SIZE) {
            await VoiceReport.insertMany(batch);
            batch = [];
            channel.ack(message, true);
        };
    } catch (error: any) {
        if (error?.name !== 'PartialFailureError') throw error;
        logger.error(`[CONSUMER](Voice Reports) PartialFailureError`);
        logger.error(JSON.stringify(error));
    }
}

export const voiceReports: IConsumer = {
    queue: QUEUE_NAME,
    processor: processMsgs
}
