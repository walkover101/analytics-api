import amqp from 'amqplib';
import logger from "../logger/logger";
import EventEmitter from 'events';
import { delay } from '../services/utility-service';
export type Connection = amqp.Connection;
export type Channel = amqp.Channel;

const RETRY_INTERVAL = 5000; // in millis
const RABBIT_CONNECTION_STRING = process.env.RABBIT_CONNECTION_STRING || '';

class RabbitConnection extends EventEmitter {
    private static instance: RabbitConnection;
    private gracefulClose: boolean = false;
    private connectionString: string;
    private connection?: Connection;

    constructor(connectionString: string) {
        super();
        if (!connectionString) throw new Error("connectionString is required");
        this.connectionString = connectionString;
        this.setupConnection();
    }

    public static getSingletonInstance(connectionString: string): RabbitConnection {
        return RabbitConnection.instance ||= new RabbitConnection(connectionString);
    }

    private async setupConnection(): Promise<Connection> {
        try {
            this.gracefulClose = false;
            this.connection = await amqp.connect(this.connectionString);
            this.initEventListeners();
            return this.connection;
        } catch (err) {
            logger.error('[RABBIT](setupConnection)', err)
            this.emit("retry");
            await delay(RETRY_INTERVAL);
            return this.setupConnection();
        }
    }

    private initEventListeners() {
        if (!this.connection) return;
        logger.info(`[RABBIT](onConnectionReady) Connection established to ${this.connectionString}`);
        this.emit("connect", this.connection);

        this.connection.on("close", (error) => {
            this.connection = undefined;

            if (this.gracefulClose) {
                logger.info('[RABBIT](onConnectionClosed) Gracefully');
                this.emit("gracefulClose");
            } else {
                logger.error('[RABBIT](onConnectionClosed) Abruptly', error);
                this.emit("error", error);
            }

            if (!this.gracefulClose) this.setupConnection();
        })
    }

    public closeConnection() {
        if (this.connection) {
            this.gracefulClose = true;
            logger.info('[RABBIT](closeConnection) Closing connection...');
            this.connection.close();
        }
    }
}

export default (connectionString: string = RABBIT_CONNECTION_STRING): RabbitConnection => {
    return RabbitConnection.getSingletonInstance(connectionString);
}