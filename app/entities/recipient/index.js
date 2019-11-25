const User = require("@app/entities/user")
const CustomError = require("@app/entities/error");

class Recipient extends User{
// _type: string

  /**
   * 
   * @param {string} id 
   * @param {string} type 
   */
  constructor(id, type){
    super(id)
    this.type = type
  }

  get type() { return this._type }
  set type(value) {
    if(!value) throw new CustomError("null-value","recipient type cannot be empty")
    if(value !== "phoneNumber" && value !== "mono" && value !== "email") throw new CustomError("null-value","recipient type must be phoneNumber, mono, or email")
    this._type = value
  }

  get data(){
    const transformedData = {}
    Object.keys(this).map((prop) => {
      const newProp = (prop.indexOf("_") === 0)? prop.substring(1): prop;
      transformedData[newProp] = this[prop];
    })
    return transformedData;
  }
}
module.exports = Recipient;