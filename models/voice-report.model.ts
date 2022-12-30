import { getStream, MSG91_DATASET_ID, MSG91_PROJECT_ID, writeClient, mode } from "../database/big-query-service";
import logger from "../logger/logger";
import protoDescriptor from "./protofiles/voiceReport_descriptor";

const parent = `projects/${MSG91_PROJECT_ID}/datasets/${MSG91_DATASET_ID}/tables/voiceTableRep`;
const writeStream = { type: mode.PENDING };
export default class VoiceReport {
    uuid: string;
    companyId: number;
    status: string; // (completed, busy, no-answer, canceled, failed)
    destination: number;
    createdAt: Date;
    startTime: Date; // string me bhej rhe hai queue se
    endTime: Date; // string me bhej rhe hai queue se
    callerId: number;
    duration: number;
    billingDuration: number;
    ivrInputs: string; 
    billing: string; // (1/1, 6/6, 60/60, 60/1, 60/6)
    rate: string;
    charged: string;
    vendorBilling: string; // (6/6, 60/60, 60/1, 60/6)
    vendorRate: string;
    vendorCharged: string;
    agentId: number;
    disconnectedBy: string; // (source, destination)
    vendorRequestId: string;

    constructor(attr: any) {
        this.uuid = attr['uuid'];
        this.companyId = attr['company_id'];
        this.status = attr['status'];
        this.destination = attr['destination'];
        this.createdAt = attr['created_at'];
        this.startTime = attr['start_time'];
        this.endTime = attr['end_time'];
        this.callerId = attr['caller_id'];
        this.duration = attr['duration'];
        this.billingDuration = attr['billing_duration'];
        this.ivrInputs = attr['ivr_inputs'];
        this.billing = attr['billing'];
        this.rate = attr['rate'];
        this.charged = attr['charged'];
        this.vendorBilling = attr['vendor_billing'];
        this.vendorRate = attr['vendor_rate'];
        this.vendorCharged = attr['vendor_charged'];
        this.agentId = attr['agent_id'];
        this.disconnectedBy = attr['disconnected_by'];
        this.vendorRequestId = attr['vendor_request_id'];

    }

    public static async insertMany(voiceReports: Array<VoiceReport>) {
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
              if (responses.length === voiceReports.length) stream.end();
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
            rows: { serializedRows: voiceReports },
          };
    
          stream.write({ writeStream: streamName, protoRows }); //send batch
        } catch (error) {
          logger.error(error);
        }
      }
}

