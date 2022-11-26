import { sequelize, DataTypes } from '../database/sequelize-service';
import { isValidObjectId } from '../services/utility-service';
import { DateTime } from 'luxon';

export enum jobType {
    REQUEST_DATA = 'requestData',
    RT_REQUEST_DATA = "rtRequestData",
    REPORT_DATA = 'reportData',
    OTP_REPORT = 'otpReport'
};

const Tracker = sequelize.define("Tracker", {
    jobType: {
        type: DataTypes.STRING,
        allowNull: false,
        primaryKey: true,
        set(value: string) {
            const types: string[] = Object.keys(jobType).map(key => jobType[key as keyof typeof jobType]);

            if (!types.includes(value)) {
                throw new Error(`Valid jobType is required - ${types.join(' | ')}`);
            }

            this.setDataValue('jobType', value);
        }
    },
    lastDocumentId: {
        type: DataTypes.STRING,
        allowNull: true,
        set(value: string) {
            if (value && !isValidObjectId(value)) {
                throw new Error('Invalid lastDocumentId');
            }

            this.setDataValue('lastDocumentId', value);
        }
    },
    lastTimestamp: {
        type: DataTypes.DATE,
        allowNull: false,
        set(value: string) {
            if (!DateTime.fromISO(value).isValid) throw new Error('Invalid lastTimestamp');

            this.setDataValue('lastTimestamp', value);
        }
    },
    token: {
        type: DataTypes.STRING,
        allowNull: true,
        set(value: string) {
            this.setDataValue('token', value);
        }
    }
});

export default Tracker;