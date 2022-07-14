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

export {
    delay,
    formatDate
}
