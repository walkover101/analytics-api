import { EventEmitter } from "stream";

class Health extends EventEmitter {
    private baseURL: string;
    private queue: Set<string> = new Set();
    constructor(baseURL: string) {
        super();
        this.baseURL = baseURL;
        this.pingServer();
    }

    ping(uuid: string) {
        if (!uuid) return;
        if (this.queue.has(uuid)) return;
        this.queue.add(uuid);
        this.emit('ping', uuid);
    }

    private pingServer() {
        const url = new URL(this.baseURL);
        this.on('ping', (uuid: string) => {
            url.pathname = uuid;
            fetch(url.toString()).finally(() => {
                this.queue.delete(uuid);
            });
        })
    }


}
// UUID : 6ceabf40-c1fd-40e9-a923-b622d97ce315

const BASE_URL = process.env.HEALTH_BASE_URL || 'https://hc-ping.com';
const health = new Health(BASE_URL);
export default health;
