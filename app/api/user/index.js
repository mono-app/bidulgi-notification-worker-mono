const admin = require("firebase-admin");

class UserAPI{

  static async getMessagingTokenById(userId){
    const db = admin.firestore();
    const documentSnapshot = await db.collection("users").doc(userId).get()
    return documentSnapshot.tokenInformation.messagingToken
  } 
}

module.exports = UserAPI;