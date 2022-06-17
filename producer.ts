import rabbitmq, { Connection } from './database/rabbitmq';
import { delay } from './utility';
const localRabbit = rabbitmq("amqp://localhost");
localRabbit.on("connection", (connection) => {
    console.log("Got Connection");
  startProducer(connection);
});
localRabbit.on("error",(error)=>{
    console.log(error);
})

function startProducer(connection: Connection){
    try {

        connection.createChannel(async (error: any, channel: any) => {
            if (error) {
                throw error;
            }
            var queue = "hello";
            channel.assertQueue(queue, {
                durable: false
            });
            let condition = true;
            channel.on("error",(error:any)=>{
                condition = false;
            })
            let i = 0;
            while (condition) {
                await delay(2000);
                var msg = "Welcome World-" + (i++);
                try{

                    channel.sendToQueue(queue, Buffer.from(msg));
                }catch(reason){
                    console.log("Error");
                    condition = false;
                }
                console.log("[x] Sent %s", msg);
            }
        })
    } catch (error) {
        console.log("Error Occurred");
    }
}