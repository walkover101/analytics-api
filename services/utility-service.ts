import { uniq } from 'lodash';
import { DateTime } from 'luxon';
import { ObjectId } from 'mongodb';
import { Stat } from '../apis'; import { CacheContainer } from 'node-ts-cache';
import { MemoryStorage } from 'node-ts-cache-storage-memory';
import logger from '../logger/logger';
import axios from 'axios';
import rabbitmqProducer from '../database/rabbitmq-producer';
import jwt from 'jsonwebtoken';
import { MSG91_DATASET_ID, MSG91_PROJECT_ID } from '../database/big-query-service';

const cache = new CacheContainer(new MemoryStorage());
const SMPP_ERROR_CODES_API = process.env.SMPP_ERROR_CODES_API || "https://control.msg91.com/api/v5/report/SMPPErrorCodeDetails";
const DEFAULT_DATE_FORMAT = 'yyyy-MM-dd';
const Hashes = require('jshashes');
const phoneUtil = require('google-libphonenumber').PhoneNumberUtil.getInstance();
const JWT_SECRET = process.env.JWT_SECRET as string;
if (!JWT_SECRET) {
    throw new Error("JWT_TOKEN is not set in env");
}
export function signToken(payload: any) {
    return jwt.sign(payload, JWT_SECRET, {
        expiresIn: '8h'
    });
}

export function verifyToken(token: string) {
    return jwt.verify(token, JWT_SECRET);
}

function delay(time = 1000) {
    return new Promise((resolve) => {
        setTimeout(() => {
            return resolve(true);
        }, time)
    });
}

function formatDate(date: string) {
    try {
        const result = DateTime.fromFormat(date, DEFAULT_DATE_FORMAT);
        if (result?.isValid) return result;
        throw `Date must be provided in '${DEFAULT_DATE_FORMAT}' format`;
    } catch (err) {
        throw `Date must be provided in '${DEFAULT_DATE_FORMAT}' format`;
    }
}

function getQuotedStrings(data: string[] | undefined) {
    if (!data || !data.length) return null;
    return "'" + data.join("','") + "'";
}

function getValidFields(permittedFields: { [key: string]: string } = {}, fields: Array<string> = []) {
    const result: { withoutAlias: string[], withAlias: string[], onlyAlias: string[] } = { withoutAlias: [], withAlias: [], onlyAlias: [] };
    let attrbs = fields.filter(field => field in permittedFields);
    if (!attrbs.length) attrbs = Object.keys(permittedFields);

    uniq(attrbs).forEach(key => {
        result.withAlias.push(`${permittedFields[key]} as ${key}`);
        result.withoutAlias.push(permittedFields[key]);
        result.onlyAlias.push(key);
    });

    return result;
}

function getHashCode(str: string) {
    return new Hashes.SHA1().hex(str);
}

function isValidObjectId(id: string) {
    try {
        return new ObjectId(id).toString() === id;
    } catch {
        return false;
    }
}

function extractCountryCode(mobileNumber: string) {
    let regionCode, countryCode = '0';

    try {
        const parsedNum = phoneUtil.parseAndKeepRawInput(`+${mobileNumber}`);
        if (!phoneUtil.isValidNumber(parsedNum)) throw '0';
        regionCode = phoneUtil.getRegionCodeForNumber(parsedNum);
        countryCode = parsedNum.getCountryCode();
    } catch (err) {
        regionCode = null;
        countryCode = '0';
    }

    return { regionCode, countryCode };
}

function getDefaultDate(dayDiff: number = 7): { from: string, to: string } {
    const today = DateTime.now();

    return {
        from: today.minus({ days: dayDiff }).toFormat(DEFAULT_DATE_FORMAT),
        to: today.toFormat(DEFAULT_DATE_FORMAT)
    };
}

function getAgeInDays(date: string) {
    return (DateTime.fromISO(date).diffNow('days').days) * -1
}

function convertCodesToMessage(caseStatement: string, codes: { [code: string | number]: any }, encodeValues: boolean = true, elseStatement?: string) {
    elseStatement = elseStatement || `CAST(${caseStatement} AS STRING)`;

    let result = `CASE ${caseStatement}`;
    Object.keys(codes).forEach(code => result += `
WHEN ${code} THEN ${encodeValues ? `"${codes[code]}"` : codes[code]}`);
    result += `
ELSE ${elseStatement} END`;

    return result;
}

async function getErrorCodes() {
    let errorCodes;

    try {
        errorCodes = await cache.getItem<string>('errorCodes');

        if (!errorCodes && SMPP_ERROR_CODES_API) {
            const response = await axios.get(SMPP_ERROR_CODES_API);
            errorCodes = response.data?.data;
            await cache.setItem('errorCodes', errorCodes, { ttl: 3600 });
        }
    } catch (error) {
        logger.error(error);
    }

    return errorCodes;
}

async function getFailureReason(smsc: string, description: string) {
    if (!smsc || !description) return;

    try {
        const error: string = description.split("err:")[1] || "";
        const code: string = error?.split(' ')[0];
        const errorCodes = await getErrorCodes();
        if (!errorCodes[smsc]) throw `[${smsc} | ${code}] Not found in error codes list`;
        return { code: code, reason: errorCodes[smsc]?.[code] };
    } catch (error) {
        logger.debug(error);
    }
}

function prepareQuery(tableName: string, attributes: string[], whereClause: string, groupBy: string) {
    const query = `SELECT ${attributes.join(',')}
            FROM \`${MSG91_PROJECT_ID}.${MSG91_DATASET_ID}.${tableName}\`
            WHERE ${whereClause}
            GROUP BY ${groupBy}`;

    return query;
}

function generateStatHTML(map: Map<string, Stat>) {
    let rows = ``;
    map.forEach((stat, key) => {
        {
            rows += `  <tr>
      <td>${key}</td>
      <td>${stat.count}</td>
      <td>${stat.avg}</td>
      <td>${stat.max}</td>
      <td>${stat.min}</td>
    </tr>`
        }
    })
    return `
    <!DOCTYPE html>
  <html>

<style>
table {
  border-collapse: collapse;
  width: 100%;
}

th, td {
  padding: 8px;
  text-align: left;
  border-bottom: 1px solid #DDD;
}

tr:hover {background-color: #D6EEEE;}
</style>


  <body>
  
  <h2 style="text-align: center;">Stats</h2>
  
  <table style="width:100%">
    <tr>
      <th>URL</th>
      <th>COUNT</th>
      <th>AVG</th>
      <th>MAX</th>
      <th>MIN</th>
    </tr>
    ${rows}
  
  </table>
  </body>
  </html>
    `
}

export async function sendChannelNotification(channelId:string, message:any){
    await rabbitmqProducer.publishToQueue("notification",{
        type: "channel",
        data: {channelId,message},
        retry: 5
    });
}
export {
    delay,
    formatDate,
    getQuotedStrings,
    getValidFields,
    isValidObjectId,
    extractCountryCode,
    getHashCode,
    getDefaultDate,
    getAgeInDays,
    convertCodesToMessage,
    generateStatHTML,
    getErrorCodes,
    getFailureReason,
    prepareQuery
}
