import amqp from 'amqplib/callback_api';
import EventEmitter from 'events';
import { delay } from '../utility';
// const connectionMap: Map<string, RabbitConnection> = new Map();

export type Connection = amqp.Connection;
class RabbitConnection extends EventEmitter {
    private connectionString: string;
    private connection?: Connection;
    private closing: Boolean = false;
    constructor(connectionString: string) {
        if (!connectionString) {
            throw new Error("connectionString is required");
        }
        super();
        this.closing = false;
        this.connectionString = connectionString;
        this.setupConnection();
    }

    private async getConnection(): Promise<Connection> {
        return new Promise((resolve, reject) => {
            amqp.connect(this.connectionString, (error: any, connection: Connection) => {
                if (error) {
                    return reject(error);
                }
                return resolve(connection);
            })
        });
    }

    private async handleConnection(connection?: Connection): Promise<Connection> {
        try {
            connection = await this.getConnection();
        } catch (error) {
            connection = undefined;
        }
        if (connection) {
            return connection;
        }
        this.emit("retry");
        await delay(5000);
        return this.handleConnection(connection);
    }

    private async setupConnection() {
        this.connection = await this.handleConnection();
        if (this.connection) {
            this.emit("connection", this.connection);
        }
        this.connection.on("error", (error) => {
            this.connection = undefined;
            this.emit("error", error);
        })
        this.connection.on("close", async (error) => {
            console.log(error);
            if (!error) {
                this.closing = true;
            }
            if (this.closing) {
                this.emit("close", error);
                return;
            }
            this.connection = undefined;
            this.setupConnection();
        })
    };

    public async closeConnection() {
        if (this.connection) {
            this.closing = true;
            this.connection.close();
        }
    }
}

// export default (connectionString: string): RabbitConnection => {
//     let connection = connectionMap.get(connectionString);
//     console.log(connectionMap);
//     if (connection) {
//         return connection;
//     }
//     connection = new RabbitConnection(connectionString);
//     connectionMap.set(connectionString, connection);
//     return connection;
// }

export default (connectionString: string): RabbitConnection =>{
    return new RabbitConnection(connectionString);
}