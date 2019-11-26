require('module-alias/register');
require('dotenv').config()
const amqp = require('amqplib')
const admin = require("firebase-admin");
const serviceAccount = require("./serviceAccountKey.json");
const NotificationAPI = require("@app/api/notification")

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
  databaseURL: "https://production-mono.firebaseio.com",
})

async function receive(){
  // receive queue from rabbit MQ

  const conn = await amqp.connect(process.env.RABBITMQ_URL);

  try{
    // Publisher
    const ch = await conn.createChannel()
    
    ch.consume("mono.queue", (msg) => {
      // do send notification to mono
      const notification = JSON.parse(msg.content)
      console.log("RECEIVE: ", notification)
      NotificationAPI.pushNotification(notification)
    })

  }catch(err){
    console.log(err.stack)
  }
}

receive();
