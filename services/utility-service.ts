import { uniq } from 'lodash';
import { DateTime } from 'luxon';
import { ObjectId } from 'mongodb';
import { Stat } from '../apis';

const DEFAULT_DATE_FORMAT = 'yyyy-MM-dd';
const Hashes = require('jshashes');
const phoneUtil = require('google-libphonenumber').PhoneNumberUtil.getInstance();

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
    generateStatHTML
}
