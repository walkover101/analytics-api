import rabbitmqService, { Connection, Channel } from '../database/rabbitmq-service';
import logger from "../logger/logger";
import VoiceRequest from '../models/voice-request.model';
import { IConsumer } from './consumer';
import { dirname } from 'path';
let protobuf = require("protobufjs");

const appDir = dirname(require.main?.filename || '');
const voiceRequest_proto = `${appDir}/models/protofiles/voice_request.proto`;
const BUFFER_SIZE = parseInt(process.env.RABBIT_VOICE_REQ_BUFFER_SIZE || '1');
const QUEUE_NAME = process.env.RABBIT_VOICE_REQ_QUEUE_NAME || 'voice-requests';
let batch: Array<VoiceRequest> = [];
let bufferLength: number = 0;
async function processMsgs(message: any, channel: Channel) {

    try {
        // let event = message?.content;
        // event = JSON.parse(event.toString());
        // protobuf.load(voiceRequest_proto, async function (err: any, root: { lookupType: (arg0: string) => any; }) {
        //     if (err) throw err;
        //     const voiceReport = root.lookupType("voiceReport.voice_report");

        //     if (Array.isArray(event)) {
        //         event.forEach(e => {
        //             let message = voiceReport.create(new VoiceRequest(e));
        //             let buffer = voiceReport.encode(message).finish();
        //             batch.push(buffer)
        //         });
        //     }
        //     if (batch.length >= BUFFER_SIZE) {
        //         await VoiceRequest.insertMany(batch);
        //         batch = [];
        //         channel.ack(message, true);
        //     }
        // });

        let event = message?.content;

        event = JSON.parse(event.toString());
        if (Array.isArray(event)) {
            event.forEach(e => batch.push(new VoiceRequest(e)));
        } else {
            batch.push(new VoiceRequest(event));
        }
        bufferLength++;
        if (bufferLength >= BUFFER_SIZE) {
            await VoiceRequest.insertMany(batch);
            batch = [];
            bufferLength = 0;
        } else {
            return;
        }
    } catch (error: any) {
        if (error?.name !== 'PartialFailureError') throw error;
        logger.error(`[CONSUMER](Voice Requests) PartialFailureError`);
        logger.error(JSON.stringify(error));
    }
    channel.ack(message, true);
}

export const voiceRequests: IConsumer = {
    queue: QUEUE_NAME,
    processor: processMsgs,
    prefetch: BUFFER_SIZE
}
