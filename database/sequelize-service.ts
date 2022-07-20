import { Sequelize, DataTypes } from 'sequelize';
import logger from '../logger/logger';

const sequelize = new Sequelize({
    dialect: 'sqlite',
    storage: 'tmp.sqlite',
});

export default async function () {
    try {
        await sequelize.sync()
        logger.info("Sequelize Synced.");
    } catch (err: any) {
        logger.error(`Failed to sync sequelize: ${err.message}`);
    }
}

export {
    sequelize,
    DataTypes
}
