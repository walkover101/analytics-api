import { DateTime } from 'luxon';

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

export {
    delay,
    formatDate,
    getQuotedStrings
}
