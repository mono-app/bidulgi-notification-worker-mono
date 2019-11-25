const { Pool } = require("pg");

class DatabaseAPI{
  static pool = null;

  /**
   * 
   * @param {function} func 
   */
  static async query(func){
    const client = await DatabaseAPI.client.connect();
    try{
      return await func(client);
    }finally{ client.release() }
  }

  static get client(){
    if(!DatabaseAPI.pool) DatabaseAPI.pool = new Pool({ connectionString: process.env.DATABASE_URL});
    return DatabaseAPI.pool;
  }
}
module.exports = DatabaseAPI;