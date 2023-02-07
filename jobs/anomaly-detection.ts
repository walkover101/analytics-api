import "../startup/dotenv";
import { getQueryResults } from "../database/big-query-service";
import rabbitmqProducer from "../database/rabbitmq-producer";
import rabbitmqService from "../database/rabbitmq-service";

const CHANNEL_ID = "63b7bb37596742002b56847c";
const NOTIFICATION_QUEUE = process.env.RABBIT_NOTIFICATION_QUEUE_NAME || 'notification';
async function anomalyDetection() {
    const query = `WITH new_data AS (
        SELECT
        DATE(sentTime) AS date,
        user_pid AS companyId,
        COUNT(DISTINCT _id) AS total_sms FROM \`msg91-reports.msg91_production.report_data_rt\`
      WHERE DATE(sentTime) >= "2023-01-18" AND DATE(sentTime) <= "2023-02-05"
      GROUP BY date, companyId
      )
      SELECT * FROM
      ML.DETECT_ANOMALIES(
        MODEL \`msg91-reports.msg91_production.arima_plus_model\`,
        STRUCT(0.9 AS anomaly_prob_threshold),
        (SELECT * FROM new_data)
      )
      WHERE is_anomaly = true AND lower_bound >= 2000
      ORDER BY companyId;`

    const data = await getQueryResults(query);
    const deviation = data.map(d => Object.assign(d, { deviation: calculateDeviation(d) }));
    let notificationMessage = `<br><br><b style="color: green;">Date : 2023-01-18 TO 2023-02-05  => Trending UP</b><br><br>`;
    const upTrend = ((deviation.filter(({ deviation }) => deviation > 0 ? true : false)).sort((a, b) => b.deviation - a.deviation));
    upTrend.length = Math.min(upTrend.length, 25);
    const downTrend = (deviation.filter(({ deviation }) => deviation < 0 ? true : false)).sort((a, b) => a.deviation - b.deviation);
    downTrend.length = Math.min(downTrend.length, 25);
    upTrend.forEach(d => notificationMessage += `<strong>${d.companyId}</strong> : ${d.deviation} <b>%</b> \n<br>`);
    notificationMessage += `<br><br><b style="color: red;">Date : 2023-01-18 TO 2023-02-05 => Trending DOWN</b><br><br>`
    downTrend.forEach(d => notificationMessage += `<strong>${d.companyId}</strong> : ${d.deviation} <b>%</b> \n<br>`);
    console.log(notificationMessage);
    sendToChannel(notificationMessage);
    // console.log(data);
}
// anomalyDetection();
export default anomalyDetection;
async function sendToChannel(message: string) {
    if (!CHANNEL_ID && !NOTIFICATION_QUEUE) {
        throw new Error("Set CHANNEL_ID and NOTIFICATION_QUEUE in env")
    }
    rabbitmqProducer.publishToQueue(NOTIFICATION_QUEUE, JSON.stringify({
        type: "channel",
        data: {
            channelId: CHANNEL_ID,
            message
        }
    }))
}

function calculateDeviation(data: any) {
    let lowerBound = data?.lower_bound;
    let upperBound = data?.upper_bound;
    let totalSms = data?.total_sms;
    let deviation = 0;
    if (totalSms < lowerBound) {
        deviation = totalSms - lowerBound;
        deviation = (deviation / lowerBound) * 100;
    }
    if (totalSms > upperBound) {
        deviation = totalSms - upperBound;
        deviation = (deviation / upperBound) * 100;
    }
    return deviation;
}