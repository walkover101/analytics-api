import { Table } from '@google-cloud/bigquery';
import msg91Dataset from '../../database/big-query-service';
import ReportData from '../../models/report-data.model';
import Download from '../../models/download.model';
import { getQuotedStrings, getValidFields } from '../utility-service';
import logger from '../../logger/logger';

const REPORT_DATA_TABLE_ID = process.env.REPORT_DATA_TABLE_ID || 'report_data'
const REQUEST_DATA_TABLE_ID = process.env.REQUEST_DATA_TABLE_ID || 'request_data'
const GCS_BUCKET_NAME = 'msg91-analytics';
const GCS_FOLDER_NAME = 'sms-exports';
const PERMITTED_FIELDS: { [key: string]: string } = {
    // from report-data
    status: 'reportData.status',
    sentTime: 'reportData.sentTime',
    deliveryTime: 'reportData.deliveryTime',
    requestId: 'reportData.requestID',
    route: 'reportData.route',
    telNum: 'reportData.telNum',
    credit: 'reportData.credit',
    senderId: 'reportData.senderID',

    // from request-data
    campaignName: 'requestData.campaign_name',
    scheduleDateTime: 'requestData.scheduleDateTime',
    msgData: 'requestData.msgData'
};

class ReportDataService {
    private static instance: ReportDataService;
    private reportDataTable: Table;

    constructor() {
        this.reportDataTable = msg91Dataset.table(REPORT_DATA_TABLE_ID);
    }

    public static getSingletonInstance(): ReportDataService {
        return ReportDataService.instance ||= new ReportDataService();
    }

    public insertMany(rows: Array<ReportData>) {
        const insertOptions = { skipInvalidRows: true, ignoreUnknownValues: true };
        return this.reportDataTable.insert(rows, insertOptions);
    }

    public download(download: Download, format: string = 'CSV') {
        logger.info('[DOWNLOAD] Creating job...');
        const exportFilePath = `gs://${GCS_BUCKET_NAME}/${GCS_FOLDER_NAME}/${download.id}_*.csv.gz`;
        const fields = getValidFields(PERMITTED_FIELDS, download.fields).join(',');
        const whereClause = this.getWhereClause(download);
        const queryStatement = `select ${fields} from ${REPORT_DATA_TABLE_ID} as reportData left join ${REQUEST_DATA_TABLE_ID} as requestData on reportData.requestId = requestData.requestId WHERE ${whereClause}`;
        logger.info(`Query: ${queryStatement}`);
        return msg91Dataset.createQueryJob({ query: this.prepareExportQuery(download.id, queryStatement, exportFilePath, format) });
    }

    private getWhereClause(download: Download) {
        const query: { [key: string]: string } = download.query || {};

        // mandatory conditions
        let conditions = `reportData.user_pid = "${download.companyId}"`;
        conditions += ` AND (DATE(reportData.sentTime) BETWEEN "${download.startDate.toFormat('yyyy-MM-dd')}" AND "${download.endDate.toFormat('yyyy-MM-dd')}")`;

        // optional conditions
        if (query.route) conditions += ` AND reportData.route in (${getQuotedStrings(query.route.split(','))})`;

        return conditions;
    }

    private prepareExportQuery(downloadId: string = '', query: string, exportPath: string, format: string) {
        return `
            BEGIN
                CREATE TEMP TABLE _SESSION.${downloadId} AS (
                    WITH temptable AS (${query})
                    SELECT * FROM temptable
                );
                
                EXPORT DATA OPTIONS(
                    uri='${exportPath}',
                    format='${format}',
                    compression='GZIP',
                    overwrite=true,
                    header=true,
                    field_delimiter=';'
                ) AS
                SELECT * FROM _SESSION.${downloadId};
            END;
        `;
    }
}

export default ReportDataService.getSingletonInstance();
