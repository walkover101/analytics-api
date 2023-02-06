// const { createCanvas, loadImage } = require('canvas');
import { merge } from "lodash";
import { DateTime, MonthNumbers } from "luxon";
import { getQueryResults, MSG91_DATASET_ID, MSG91_PROJECT_ID, OTP_TABLE_ID, REPORT_DATA_TABLE_ID } from "../../database/big-query-service";
import { createBarChart, createDoughnutChart, createPieChart } from "./graph";

// const SVGtoPNG = require('svg-to-png');
const fs = require('fs');
// const Canvas = require('canvas');
// var svg2png = require("svg2png");
const PDFDocument = require('pdfkit');

let date = 'JAN 2023';
let name = 'VIJAY';
let promotional = 35;
let transactional = 180;
let otp = 90;
let percentPromo = 10;
let percentTrans = 25;
let percentOtp = 12;



function convertString(num: number) {
    if (num < 1000) return num.toString();
    const result = num / 1000;
    return result.toFixed(1) + 'K';
}

export function generatePDF(companyId: string, startDate: DateTime, endDate: DateTime): Promise<Buffer> {
    return new Promise(async (resolve, reject) => {
        name = companyId;
        date = `${startDate.monthShort.toUpperCase()} ${startDate.year}`;
        const otpData = await getOTPReport(companyId, startDate, endDate);
        otp = otpData.reduce((acc: number, cur: any) => acc + cur.count, 0);
        const smsData = await getCompanyReport(companyId, startDate, endDate);
        promotional = smsData.reduce((acc: number, cur: any) => acc + cur.count, 0);
        const data = [...smsData, ...otpData];
        console.log(getDateCount(data));
        console.log(getCountryCount(data));
        console.log(getSMSCCount(data))
        const doc = new PDFDocument({
            size: 'A4',
            margins: {
                top: 24,
                bottom: 24,
                left: 24,
                right: 24
            },

        });
        const pageWidth = 595.28;
        const pageHeight = 841.89;
        const margin = 24;
        // const stream = doc.pipe(fs.createWriteStream('output.pdf'));
        const buffer: any[] = [];
        doc.on('data', (chunk: any) => {
            buffer.push(chunk);
        });
        doc.on('end', () => {
            return resolve(Buffer.concat(buffer));
        });
        // Report Header
        doc.fillColor('#1A66A5')
            .font('./asset/fonts/Regular.ttf')
            .fontSize(11).text('SMS|Email|RCS|Voice|Whatsapp|Short URL', {
                align: 'right'
            })
            .fontSize(11).text('Segmento|Campaign|Hello|Send OTP', {
                align: 'right'
            })
            .moveUp(3)
            .image('./asset/images/Wlogo.png', {
                fit: [130, 100],
                align: 'left'
            })
            .underline(0, 10, pageWidth, 65, { color: '#1A66A5' })
            .fillColor('black')
        // Report Title
        doc.fontSize(22).text('MONTHLY REPORT ', { lineBreak: false })
            .font('./asset/fonts/Bold.ttf')
            .fontSize(22).text(`- ${date}`, { lineBreak: true })
            .font('./asset/fonts/Italic.ttf')
            .fillColor('#494F56')
            .fontSize(22).text(`FOR ${name} SALES`, 24, doc.y, { align: 'left' })
            .lineJoin('square')
        // Report Body
        const curPos = doc.y + 24;
        const boxWidth = 165;
        const boxHeight = 80;
        const side = 182;
        const column = {
            one: margin,
            two: margin + side + 10,
            three: margin + (side * 2) + 20
        }
        doc.rect(column.one, curPos, boxWidth, boxHeight)
            .rect(column.two, curPos, boxWidth, boxHeight)
            .rect(column.three, curPos, boxWidth, boxHeight)
            .fillOpacity(0.5)
            .fillAndStroke("#E8F4FF", "#E8F4FF")
            .lineJoin('square')
            .rect(column.one, 240, boxWidth, 20)
            .rect(column.two, 240, boxWidth, 20)
            .rect(column.three, 240, boxWidth, 20)
            .fillOpacity(.8)
            .fillAndStroke("#1A66A5", "#1A66A5")

            .font('./asset/fonts/Regular.ttf')
            .fillColor('#1A66A5').fontSize(10)
            .text('PROMOTIONAL', column.one + 16, 180)
            .text('TRANSACTIONAL', column.two + 16, 180)
            .text('OTP', column.three + 16, 180)


            .font('./asset/fonts/Bold.ttf')
            .fontSize(30)
            .text(`${convertString(promotional)}`, column.one + 16, 195)
            .text(`${convertString(transactional)}`, column.two + 16, 195)
            .text(`${convertString(otp)}`, column.three + 16, 195)

            .fontSize(9)
            .fillColor('#d20100').text(`${percentPromo}% `, column.one + 32, 243, { lineBreak: false }).fillColor('white').text('from last month')
            .fillColor('#90EE90').text(`${percentTrans}% `, column.two + 32, 243, { lineBreak: false }).fillColor('white').text('from last month')
            .fillColor('#90EE90').text(`${percentOtp}% `, column.three + 32, 243, { lineBreak: false }).fillColor('white').text('from last month')
            .moveDown()
            // if increment from last data then green else red
            .image('./asset/images/down.jpg', column.one + 16, 246, { fit: [8, 8] })
            .image('./asset/images/up.jpg', column.two + 16, 246, { fit: [8, 8] })
            .image('./asset/images/up.jpg', column.three + 16, 245, { fit: [8, 8] })

        const [dateLabel, dateCount] = [getDateCount(data).map(d => d.date), getDateCount(data).map(d => d.count)];
        const [countryLabel, countryCount] = [getCountryCount(data).map(d => d.country), getCountryCount(data).map(d => d.count)];
        const [smscLabel, smscCount] = [getSMSCCount(data).map(d => d.sender), getSMSCCount(data).map(d => d.count)];
        doc.image(createBarChart(dateLabel, dateCount, { width: 1120, height: 540 }), margin, 290, { fit: [pageWidth - (2 * margin), 270] })
            .image(createDoughnutChart(countryLabel, countryCount, { width: 720, height: 720 }), margin, 600, { fit: [200, 200] })
            .image(createDoughnutChart(smscLabel, smscCount, { width: 720, height: 720 }), (pageWidth - 200 - margin), 600, { fit: [200, 200] })
        // Report Footer
        doc.x = 24;
        doc.y = pageHeight - 24;
        doc.lineJoin('square')
            .rect(0, doc.y, pageWidth, 24)
            .fillOpacity(0.9)
            .fillAndStroke("#1A66A5", "#1A66A5")

            .fillColor('white')
            .fontSize(9).text(`WALKOVER WEB SOLUTIONS PVT. LTD.`, doc.x, doc.y + 4, {
                align: 'center',
                width: pageWidth - 48,
                height: 16,
                lineBreak: false,
                valign: 'center'
            })
        doc.end();
    });
}
type ReportData = {
    date: string;
    count: number;
    country: string;
    sender: string;
};
async function getCompanyReport(companyId: string, startDate: DateTime, endDate: DateTime) {
    const sql = `SELECT STRING(DATE(sentTime)) as date,COUNT(DISTINCT _id) as count,ANY_VALUE(countryCode) as country, ANY_VALUE(senderID) as sender FROM \`${MSG91_PROJECT_ID}.${MSG91_DATASET_ID}.${REPORT_DATA_TABLE_ID}\`
     WHERE user_pid = '${companyId}' AND (sentTime BETWEEN "${startDate.setZone('utc').toFormat("yyyy-MM-dd HH:mm:ss z")}" AND "${endDate.setZone('utc').toFormat("yyyy-MM-dd HH:mm:ss z")}")
     GROUP BY date,countryCode,senderID
     ORDER BY date;`;
    const [data, meta] = await getQueryResults(sql, true);
    console.log(meta);
    console.log(data);
    return data;
}

async function getOTPReport(companyId: string, startDate: DateTime, endDate: DateTime) {
    const sql = `SELECT STRING(DATE(sentTime)) as date,COUNT(DISTINCT id) as count,ANY_VALUE(countryCode) as country, ANY_VALUE(requestSender) as sender FROM \`${MSG91_PROJECT_ID}.${MSG91_DATASET_ID}.${OTP_TABLE_ID}\`
     WHERE requestUserid = '${companyId}' AND (sentTime BETWEEN "${startDate.setZone('utc').toFormat("yyyy-MM-dd HH:mm:ss z")}" AND "${endDate.setZone('utc').toFormat("yyyy-MM-dd HH:mm:ss z")}")
     GROUP BY date,countryCode,requestSender
     ORDER BY date;`;
    const [data, meta] = await getQueryResults(sql, true);
    console.log(meta);
    console.log(data);
    return data;
}

function getDateCount(data: ReportData[]) {
    const dateCount = new Map<string, number>();
    data.forEach((d) => {
        let date = d.date?.toUpperCase() || 'null';
        if (dateCount.has(date)) {
            dateCount.set(date, (dateCount.get(date) || 0) + d.count);
        } else {
            dateCount.set(date, d.count);
        }
    });

    return Array.from(dateCount).map(([date, count]) => { return { date, count } });
}
function getCountryCount(data: ReportData[]) {
    const countryCount = new Map<string, number>();
    data.forEach((d) => {
        let country = d.country?.toUpperCase() || 'null';
        if (countryCount.has(country)) {
            countryCount.set(country, (countryCount.get(country) || 0) + d.count);
        } else {
            countryCount.set(country, d.count);
        }
    });

    return Array.from(countryCount).map(([country, count]) => { return { country, count } });
};

function getSMSCCount(data: ReportData[]) {
    const smscCount = new Map<string, number>();
    data.forEach((d) => {
        let smsc = d.sender?.toUpperCase() || 'null';
        if (smscCount.has(smsc)) {
            smscCount.set(smsc, (smscCount.get(smsc) || 0) + d.count);
        } else {
            smscCount.set(smsc, d.count);
        }
    });

    return Array.from(smscCount).map(([sender, count]) => { return { sender, count } });
}