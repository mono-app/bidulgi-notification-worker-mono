const CustomError = require("@app/entities/error");

class Password{
  // value: string
  
  /**
   * 
   * @param {string} value 
   */
  constructor(value){
    if(!value) throw new CustomError("password/empty", "Password is empty");
    if(!value.trim()) throw new CustomError("password/enpty", "Password is empty");
    if(value.length < 6) throw new CustomError("password/weak", "Password must be more than 6");
    this.value = value;
  }
}
module.exports = Password;