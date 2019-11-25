require('module-alias/register');
require('dotenv').config()
const amqp = require('amqplib')
const NotificationAPI = require("@app/api/notification");
const QueueAPI = require("@app/api/queue");
const StatusUtils = require("@app/utils/status");

async function scheduleNotifications(){
  // send to rabbit MQ queue

  const notifications = await NotificationAPI.list(StatusUtils.QUEUEING)
  const conn = await amqp.connect(process.env.RABBITMQ_URL);

  try{
    // Publisher
    const ch = await conn.createChannel()

    for(const notification of notifications){
      const queueName = QueueAPI.getQueue(notification.recipient.type);

      await NotificationAPI.updateStatusToSendingById(notification.id)
      const buffer = Buffer.from(JSON.stringify(notification))
      await ch.sendToQueue(queueName, buffer)
    }
  }catch(err){
    console.log(err.stack)
  }finally{
    conn.close()
  }
}

function start(){
 setTimeout(async () => {
  await scheduleNotifications()
  start()
 }, 1000)
}

start();