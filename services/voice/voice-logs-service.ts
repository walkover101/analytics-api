import { getQueryResults, MSG91_DATASET_ID, MSG91_PROJECT_ID, VOICE_REP_TABLE_ID, VOICE_REQ_TABLE_ID } from '../../database/big-query-service';
import { convertCodesToMessage, getQuotedStrings, getValidFields, signToken, verifyToken } from '../utility-service';
import { DateTime } from 'luxon';
import logger from '../../logger/logger';
import { PaginationOption } from '../email/mail-logs-service';

const DEFAULT_TIMEZONE: string = 'Asia/Kolkata';
const PERMITTED_FIELDS: { [key: string]: string } = {
    // from report-data
    createdAt: `STRING(TIMESTAMP_TRUNC(DATETIME(reportData.createdAt,'${DEFAULT_TIMEZONE}'), SECOND))`,
    status: 'reportData.status',
    startTime: 'STRING(DATE(reportData.startTime))',
    endTime: 'STRING(TIME(reportData.endTime))',
    duration: 'reportData.duration',
    billingDuration: 'reportData.billingDuration',
    charged: 'reportData.charged',
    destination: 'reportData.destination'
    // from request-data
};

class VoiceLogsService {
    private static instance: VoiceLogsService;


    public static getSingletonInstance(): VoiceLogsService {
        return VoiceLogsService.instance ||= new VoiceLogsService();
    }

    public getQuery(companyId: string, startDate: DateTime, endDate: DateTime, timeZone: string = DEFAULT_TIMEZONE, filters: { [key: string]: string } = {}, fields: string[] = []) {
        startDate = startDate.setZone(timeZone).set({ hour: 0, minute: 0, second: 0 });
        endDate = endDate.plus({ days: 1 }).setZone(timeZone).set({ hour: 0, minute: 0, second: 0 });
        const attributes = getValidFields(PERMITTED_FIELDS, fields).withAlias.join(',');
        const whereClause = this.getWhereClause(companyId, startDate, endDate, timeZone, filters);
        const query = `SELECT ${attributes} FROM \`${MSG91_PROJECT_ID}.${MSG91_DATASET_ID}.${VOICE_REQ_TABLE_ID}\` as requestData
            LEFT JOIN \`${MSG91_PROJECT_ID}.${MSG91_DATASET_ID}.${VOICE_REP_TABLE_ID}\` as reportData
            ON requestData.uuid = reportData.uuid
            WHERE ${whereClause}`;
        logger.info(query);
        return query;
    }

    private getWhereClause(companyId: string, startDate: DateTime, endDate: DateTime, timeZone: String, filters: { [key: string]: string }) {
        const query: { [key: string]: string } = filters;

        // mandatory conditions
        let conditions = `(reportData.createdAt BETWEEN "${startDate.setZone('utc').toFormat("yyyy-MM-dd HH:mm:ss z")}" AND "${endDate.plus({ days: 1 }).setZone('utc').toFormat("yyyy-MM-dd HH:mm:ss z")}")`;
        conditions += ` AND (requestData.createdAt BETWEEN "${startDate.setZone('utc').toFormat("yyyy-MM-dd HH:mm:ss z")}" AND "${endDate.setZone('utc').toFormat("yyyy-MM-dd HH:mm:ss z")}")`;

        // optional conditions
        if (companyId) conditions += `AND reportData.companyId = ${companyId} AND requestData.companyId = ${companyId}`;
        return conditions;
    }

    public async getLogs(companyId: string, startDate: DateTime, endDate: DateTime, timeZone: string = DEFAULT_TIMEZONE, filters: { [key: string]: string } = {}, fields: string[] = [],option?:PaginationOption) {
        let query: string = this.getQuery(companyId, startDate, endDate, timeZone, filters, fields);
        const pagination = (option?.paginationToken) ? verifyToken(option?.paginationToken) as PaginationOption : {};
        if (option && pagination?.datasetId && pagination?.tableId) {
            // Set default values
            option.limit = option?.limit || 100;
            option.offset = option?.offset || 0;
            query = `SELECT * FROM \`${MSG91_PROJECT_ID}.${pagination?.datasetId}.${pagination?.tableId}\` LIMIT ${option.limit} OFFSET ${option?.offset}`;
        }
        let [data, metadata] = await getQueryResults(query, true);
        if (option?.limit) data = data?.slice(0, option?.limit);

        return {
            data, metadata: {
                ...metadata,
                offset: option?.offset,
                limit: option?.limit,
                paginationToken: signToken({
                    tableId: pagination?.tableId || metadata?.tableId,
                    datasetId: pagination?.datasetId || metadata?.datasetId,
                })
            }
        };
    }
}

export default VoiceLogsService.getSingletonInstance();