import amqp from 'amqplib';
import logger from "../logger/logger";
import EventEmitter from 'events';
import { delay } from '../services/utility-service';
export type Connection = amqp.Connection;

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
            logger.error('[RABBIT](getConnection)', err)
            this.emit("retry");
            await delay(RETRY_INTERVAL);
            return this.setupConnection();
        }
    }

    private initEventListeners() {
        if (!this.connection) return;
        this.emit("connect", this.connection)

        this.connection.on("close", (error) => {
            logger.error('[RABBIT](onConnectionClose)', error)
            this.connection = undefined;

            if (error) {
                this.emit("error", error)
            } else {
                this.emit("gracefulClose");
            }

            if (!this.gracefulClose) this.setupConnection();
        })
    }

    public closeConnection() {
        if (this.connection) {
            this.gracefulClose = true;
            this.connection.close();
        }
    }
}

export default (connectionString: string = RABBIT_CONNECTION_STRING): RabbitConnection => {
    return RabbitConnection.getSingletonInstance(connectionString);
}