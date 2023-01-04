import msg91Dataset, { getStream, MSG91_DATASET_ID, MSG91_PROJECT_ID, writeClient, mode, VOICE_REQ_TABLE_ID } from "../database/big-query-service";
import logger from "../logger/logger";
import { Table } from '@google-cloud/bigquery';
import protoDescriptor from "./protofiles/voiceRequest_descriptor";


const voiceRequestTable: Table = msg91Dataset.table(VOICE_REQ_TABLE_ID);
const parent = `projects/${MSG91_PROJECT_ID}/datasets/${MSG91_DATASET_ID}/tables/voiceTableReq`;
const writeStream = { type: mode.PENDING };
export default class VoiceRequest {
  uuid: string;
  companyId: number;
  status: string; // (queued, ringing, balance)
  createdAt: Date;
  boxId: number;
  source: number; //(api, hello)
  destination: number;
  direction: string; // (inbound, outbound)
  connectType: string; // Direct|Team|Flow|agent|tts
  templateId: number;
  type: string; // ('api','bulk','hello','trunk','utteru')
  startTime: Date;
  callerId: number;
  connectedTo: number;
  agentId: number;
  uuidBulk: string;
  campaignNodeId: number;
  vendorBulkId: string;
  vendorRequestId: string;

  constructor(attr: any) {
    this.uuid = attr['uuid'];
    this.companyId = attr['company_id'];
    this.status = attr['status'];
    this.createdAt = attr['created_at'];
    this.boxId = attr['box_id'];
    this.source = attr['source'];
    this.destination = attr['destination'];
    this.direction = attr['direction'];
    this.connectType = attr['connect_type'];
    this.templateId = attr['template_id'];
    this.type = attr['type'];
    this.startTime = attr['start_time'];
    this.callerId = attr['caller_id'];
    this.connectedTo = attr['connected_to'];
    this.agentId = attr['agent_id'];
    this.uuidBulk = attr['bulk_uuid'];
    this.campaignNodeId = attr['campaign_node_id'];
    this.vendorBulkId = attr['vendor_bulk_id'];
    this.vendorRequestId = attr['vendor_request_id'];
  }

  public static async newInsertMany(voiceRequests: Array<VoiceRequest>) {
    try {
      const [stream, streamName] = await getStream(parent, writeStream);
      const responses: any = [];

      stream.on(
        "data",
        (response: { error: { message: string | undefined } }) => {
          if (response.error) {
            throw new Error(response.error.message);
          }

          responses.push(response);
          if (responses.length === voiceRequests.length) stream.end();
        }
      );

      stream.on("error", (err: any) => {
        throw err;
      });

      stream.on("end", async () => {
        try {
          let [response] = await writeClient.finalizeWriteStream({
            // after this unable to append any data
            name: streamName,
          });

          logger.info(`Row count: ${response.rowCount}`);

          [response] = await writeClient.batchCommitWriteStreams({
            // after this data will be available to read
            parent,
            writeStreams: [streamName],
          });

          logger.info(response);
        } catch (err) {
          logger.error(err);
        }
      });

      let protoRows: Object = {
        writerSchema: { protoDescriptor },
        rows: { serializedRows: voiceRequests },
      };

      stream.write({ writeStream: streamName, protoRows }); //send batch
    } catch (error) {
      logger.error(error);
    }
  }

  public static insertMany(rows: Array<VoiceRequest>) {
    const insertOptions = { skipInvalidRows: true, ignoreUnknownValues: true };
    return voiceRequestTable.insert(rows, insertOptions);
  }
}