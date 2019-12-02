const CustomError = require("@app/entities/error");

class User{
  // _id: string;

  /**
   * 
   * @param {string} id 
   */
  constructor(id){
    this.id = id? id: null;
  }

  get id() { return this._id }
  set id(value) {
    if(!value) throw new CustomError("null-value", "user id cannot empty")
    this._id = value
  }
}
module.exports = User;