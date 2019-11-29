const PhoneNumber = require("@app/entities/phoneNumber")
const Email = require("@app/entities/email")
const CustomError = require("@app/entities/error");

class Recipient{
// _id: string
// _type: string

  /**
   * 
   * @param {string} id 
   * @param {string} type 
   */
  constructor(id, type){
    this.type = type
    this.id = id
  }

  get id() { 
    if(this.type==="phoneNumber") return this._id.number
    else if(this.type==="email") return this._id.address
    else return this._id 
  }
  set id(value) {
    if(!value) throw new CustomError("null-value","recipient id cannot be empty")
    if(this.type==="phoneNumber") this._id = new PhoneNumber(value, true)
    else if(this.type==="email") this._id = new Email(value)
    else this._id = value
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
      transformedData[newProp] = this[newProp];
    })
    return transformedData;
  }
}
module.exports = Recipient;