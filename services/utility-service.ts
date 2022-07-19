import { DateTime } from 'luxon';
import { intersection } from 'lodash';

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

function getValidFields(permittedFields: { [key: string]: string }, fields: Array<string> = []) {
    if (!permittedFields || !fields.length) return [];
    const result: string[] = [];
    let attrbs = intersection(Object.keys(permittedFields), fields);
    if (!attrbs.length) attrbs = Object.keys(permittedFields);
    attrbs.map(key => result.push(permittedFields[key]));
    return result;
}

export {
    delay,
    formatDate,
    getQuotedStrings,
    getValidFields
}
