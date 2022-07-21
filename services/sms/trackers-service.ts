import logger from "../../logger/logger";
import Tracker, { jobType } from "../../models/trackers.model";

async function get(job: jobType) {
    return Tracker.findByPk(job);
}

function upsert(job: jobType, lastDocumentId: any) {
    logger.info(`[UPDATE TRACKERS] Updating lastDocumentId to ${lastDocumentId}...`);
    return Tracker.upsert({ jobType: job, lastDocumentId });
}

export {
    get,
    upsert
}