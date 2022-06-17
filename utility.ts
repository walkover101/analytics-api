import { DateTime } from "luxon";
export function getDefaultDate(dayDiff: number = 7): { start: string, end: string } {
    const start = DateTime.now();
    const end = start.minus({ days: dayDiff });
    return { start: start.toFormat('yyyy-MM-dd'), end: end.toFormat('yyyy-MM-dd') };
}
export function delay(time = 1000) {
    return new Promise((resolve) => {
        setTimeout(() => {
            return resolve(true);
        }, time)
    });
}