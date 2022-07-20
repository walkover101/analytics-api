import { sequelize, DataTypes } from '../database/sequelize-service';

const Tracker = sequelize.define("Tracker", {
    jobType: DataTypes.ENUM('REQUEST_DATA', 'REPORT_DATA'),
    lastDocumentId: DataTypes.STRING
});

export default Tracker;