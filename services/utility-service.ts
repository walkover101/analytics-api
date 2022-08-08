import { DateTime } from 'luxon';
import { intersection } from 'lodash';
import { ObjectId } from 'mongodb';
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
        return DateTime.fromFormat(date, 'dd-MM-yyyy');
    } catch (err) {
        return null;
    }
}

function getQuotedStrings(data: string[] | undefined) {
    if (!data || !data.length) return null;
    return "'" + data.join("','") + "'";
}

function getValidFields(permittedFields: { [key: string]: string } = {}, fields: Array<string> = []) {
    const result: { withoutAlias: string[], withAlias: string[] } = { withoutAlias: [], withAlias: [] };
    let attrbs = intersection(Object.keys(permittedFields), fields);
    if (!attrbs.length) attrbs = Object.keys(permittedFields);

    attrbs.forEach(key => {
        result.withAlias.push(`${permittedFields[key]} as ${key}`);
        result.withoutAlias.push(permittedFields[key]);
    });

    return result;
}

function isValidObjectId(id: string) {
    try {
        return new ObjectId(id).toString() === id;
    } catch {
        return false;
    }
}

function extractCountryCode(mobileNumber: string) {
    let country, countryCode = '0';

    try {
        const parsedNum = phoneUtil.parseAndKeepRawInput(`+${mobileNumber}`);
        if (!phoneUtil.isValidNumber(parsedNum)) throw 'INVALID';
        country = phoneUtil.getRegionCodeForNumber(parsedNum);
        countryCode = parsedNum.getCountryCode();
    } catch (err) {
        country = 'INVALID';
        countryCode = '0';
    }

    return { country, countryCode };
}

export {
    delay,
    formatDate,
    getQuotedStrings,
    getValidFields,
    isValidObjectId,
    extractCountryCode
}
