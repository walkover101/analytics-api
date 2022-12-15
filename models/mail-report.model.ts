import { getStream, MSG91_PROJECT_ID, writeClient, MSG91_DATASET_ID, MAIL_REP_TABLE_ID, mode } from '../database/big-query-service';
import { getHashCode } from "../services/utility-service";
import protoDescriptor from './protofiles/mailReport_descriptor';
import { DateTime } from 'luxon';

const parent = `projects/${MSG91_PROJECT_ID}/datasets/${MSG91_DATASET_ID}/tables/${MAIL_REP_TABLE_ID}`;
const writeStream = { type: mode.PENDING };
export default class MailReport {
  requestId: string; //Request Id of this mail (Not unique in this table)
  eventId: number; //Events that occurred while processing this mail. (2,3,4,8,9)	
  statusCode: number; //Standard Response Code of SMTP Protocol	
  enhancedStatusCode: string; //Standard Response Code of SMTP Protocol	
  reason: string; //Description of Response Status	
  resultState: string;
  remoteMX: string;
  remoteIP: string; //MX Server IP of Client	
  contentSize: number;
  senderDedicatedIPId: number; //Unique id of Dedicated IP used to send this mail.
  hostname: string;
  recipientEmail: string; //Recipient Email Address
  outboundEmailId: number; //Unique Id is generated for each request (All recipients, cc, bcc of that mail will have same id) in MySQL.
  companyId: string; //Id of the company which requested this mail.
  requestTime: string; //Timestamp when this mail was requested to be sent.	
  createdAt: string; //Time when this specific event happened

  constructor(attr: any) {
    this.eventId = parseInt(attr['eid']);
    this.statusCode = parseInt(attr['stc']);
    this.enhancedStatusCode = attr['esc'];
    this.reason = attr['rsn'];
    this.resultState = attr['rst'];
    this.remoteMX = attr['rmx'];
    this.remoteIP = attr['rip'];
    this.contentSize = parseInt(attr['csz']);
    this.senderDedicatedIPId = parseInt(attr['sid']);
    this.hostname = attr['hnm'];

    //common in all three email models
    this.recipientEmail = attr['rem']?.toLowerCase();
    this.outboundEmailId = parseInt(attr['oid']);
    this.requestId = getHashCode(`${attr['mri']}-${this.recipientEmail}`);
    this.companyId = attr['cid'];
    this.requestTime = DateTime.fromJSDate(new Date(attr['mct'] || null)).toString();
    this.createdAt = DateTime.fromJSDate(new Date(attr['created_at'] || null)).toString();
  }

  public static async insertMany(mailReports: Array<MailReport>) {
    try {
      const stream = await getStream(parent, writeStream);
      const responses: any = [];

      stream.on('data', (response: { error: { message: string | undefined; }; }) => {
        if (response.error) {
          throw new Error(response.error.message);
        }

        responses.push(response);
        if (responses.length === mailReports.length) stream.end();
      });

      stream.on('error', (err: any) => {
        throw err;
      });

      stream.on('end', async () => {
        try {
          let [response] = await writeClient.finalizeWriteStream({ // after this unable to append any data
            name: writeStream,
          });

          console.info(`Row count: ${response.rowCount}`);

          [response] = await writeClient.batchCommitWriteStreams({ // after this data will be available to read
            parent,
            writeStreams: [writeStream],
          });

          console.info(response);
        } catch (err) {
          console.error(err);
        }
      });

      let protoRows: Object = {
        writerSchema: { protoDescriptor },
        rows: { serializedRows: mailReports },
      };

      stream.write({ writeStream, protoRows }); //send batch
    } catch (error) {
      console.error(error);
    }
  }
}