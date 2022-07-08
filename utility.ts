import { DateTime } from "luxon";
export function getDefaultDate(dayDiff: number = 7): { start: string, end: string } {
    const start = DateTime.now();
    const end = start.minus({ days: dayDiff });
    return { start: start.toFormat('yyyy-MM-dd'), end: end.toFormat('yyyy-MM-dd') };
}
