const admin = require("firebase-admin");
const User = require("@app/entities/user")

class UserAPI{
  static async getDetailById(userId){
    const db = admin.firestore();
    const documentSnapshot = await db.collection("users").doc(userId).get()
    return User.fromSnapshot(documentSnapshot)
  }
}

module.exports = UserAPI;