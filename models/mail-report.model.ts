import  { mode, parent, writeClient } from '../database/big-query-service';
import { getHashCode } from "../services/utility-service";
import protoDescriptor from './protofiles/mailReport_descriptor';
import { DateTime } from 'luxon';
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
        let writeStream = {type: mode.PENDING};
        let request: any = {
            parent,
            writeStream,
        };
        let [response] = await writeClient.createWriteStream(request); //stream is created
        console.log(`Stream created: ${response.name}`);

        writeStream = response.name;
        const options: {otherArgs: any} = {otherArgs: { headers: []}};
        options.otherArgs.headers[
            'x-goog-request-params'
        ] = `write_stream=${writeStream}`;  // This header is required so that BQ storage API knows which region to route the request to

        const stream = await writeClient.appendRows(options); 
        const responses: any = [];

        stream.on('data', (response: { error: { message: string | undefined; }; }) => {
            if (response.error) {
              throw new Error(response.error.message);
            }
            console.log(response);
            responses.push(response);
            if (responses.length) {
              stream.end();
            }
        });

        stream.on('error', (err: any) => {
          throw err;
        });

        stream.on('end', async () => {
          try {
            [response] = await writeClient.finalizeWriteStream({ // after this unable to append any data
              name: writeStream,
            });
            console.log(`Row count: ${response.rowCount}`);
  
            [response] = await writeClient.batchCommitWriteStreams({ // after this data will be available to read
              parent,
              writeStreams: [writeStream],
            });
            console.log(response);
          } catch (err) {
            console.log(err);
          }
        });

        let protoRows: Object = {
          writerSchema: {protoDescriptor},
          rows: {serializedRows: mailReports},
        };

        request = {
          writeStream,
          protoRows,
        };
        stream.write(request); //send batch
    
      } catch (error) {
        console.log(error);
      }
    }
}