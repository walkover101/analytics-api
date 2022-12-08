// import { Table } from '@google-cloud/bigquery';
import  { MAIL_REP_TABLE_ID, MSG91_DATASET_ID, MSG91_PROJECT_ID } from '../database/big-query-service';
import { getHashCode } from "../services/utility-service";
const {BigQueryWriteClient} = require('@google-cloud/bigquery-storage').v1;

const writeClient = new BigQueryWriteClient({});

const type = require('@google-cloud/bigquery-storage').protos.google.protobuf
  .FieldDescriptorProto.Type;
const mode = require('@google-cloud/bigquery-storage').protos.google.cloud
  .bigquery.storage.v1.WriteStream.Type;

// const mailReportTable: Table = msg91Dataset.table(MAIL_REP_TABLE_ID);
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
    requestTime: Date; //Timestamp when this mail was requested to be sent.	
    createdAt: Date; //Time when this specific event happened

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
        this.requestTime = attr['mct'] && new Date(attr['mct']);
        this.createdAt = attr['created_at'] && new Date(attr['created_at']);
    }

    public static async insertMany(mailReports: Array<MailReport>) {
    const protoDescriptor: {name: string, field: any[]} = {name: '', field: []};
  protoDescriptor.name = MAIL_REP_TABLE_ID;
  protoDescriptor.field = [
    {
      name: 'requestId',
      number: 1,
      type: type.TYPE_STRING,
    },
    {
        name: 'eventId',
        number: 2,
        type: type.TYPE_NUMBER,
      },
      {
        name: 'statuscode',
        number: 3,
        type: type.TYPE_NUMBER,
      },
      {
        name: 'enhancedStatusCode',
        number: 4,
        type: type.TYPE_STRING,
      },
      {
        name: 'reason',
        number: 5,
        type: type.TYPE_STRING,
      },
      {
        name: 'resultState',
        number: 6,
        type: type.TYPE_STRING,
      },
      {
        name: 'remoteMX',
        number: 7,
        type: type.TYPE_STRING,
      },
      {
        name: 'remoteIP',
        number: 8,
        type: type.TYPE_STRING,
      },
      {
        name: 'contentSize',
        number: 9,
        type: type.TYPE_NUMBER,
      },
      {
        name: 'senderDedicatedIPId',
        number: 10,
        type: type.TYPE_NUMBER,
      },
      {
        name: 'hostname',
        number: 11,
        type: type.TYPE_STRING,
      },
      {
        name: 'recipientEmail',
        number: 12,
        type: type.TYPE_STRING,
      },
      {
        name: 'outboundEmailId',
        number: 13,
        type: type.TYPE_STRING,
      },
      {
        name: 'companyId',
        number: 14,
        type: type.TYPE_STRING,
      },
      {
        name: 'requestTime',
        number: 15,
        type: type.TYPE_DATE,
      },
      {
        name: 'createdAt',
        number: 16,
        type: type.TYPE_DATE,
      },
  ];


  const  projectId: string = MSG91_PROJECT_ID;
  const datasetId: string = MSG91_DATASET_ID;
  const tableId: string = MAIL_REP_TABLE_ID

  const parent = `projects/${projectId}/datasets/${datasetId}/tables/${tableId}`;
 
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

    const stream = await writeClient.appendRows(options); // append data to stream

    const responses: any = [];

    stream.on('data', (response: { error: { message: string | undefined; }; }) => {
        // Check for errors.
        if (response.error) {
          throw new Error(response.error.message);
        }
  
        console.log(response);
        responses.push(response);
  
        // Close the stream when all responses have been received.
        if (responses.length === 10) {
          stream.end();
        }
      });

      stream.on('error', (err: any) => {
        throw err;
      });

      stream.on('end', async () => {
        // API call completed.
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
        rows: {mailReports},
      };

      request = {
        writeStream,
        protoRows,
      };

      stream.write(request); //send batch
      return protoRows;
    
  } catch (error) {
    console.log(error);
  }

}
}