import logger from "../../logger/logger";
import Tracker, { jobType } from "../../models/trackers.model";

async function get(job: jobType) {
    try {
        return await Tracker.findByPk(job);
    } catch (err: any) {
        logger.error(`Unable to fetch tracker ${err.message}`);
        return null
    }
}

async function upsert(job: jobType, lastDocumentId: any) {
    // const lastDocId = lastDocument?._id?.toString() || null;
    return await Tracker.upsert({ jobType: job, lastDocumentId });
}

export {
    get,
    upsert
}