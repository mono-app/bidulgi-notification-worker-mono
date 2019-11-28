const User = require("@app/entities/user")
const CustomError = require("@app/entities/error");

class Creator extends User{
// _provider: string

  /**
   * 
   * @param {string} id 
   * @param {string} provider 
   */
  constructor(id, provider){
    super(id)
    this.provider = provider
  }
  
  get provider() { return this._provider }
  set provider(value) {
    if(!value) throw new CustomError("null-value","creator provider type cannot empty")
    this._provider = value
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
module.exports = Creator;