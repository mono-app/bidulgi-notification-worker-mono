const CustomError = require("@app/entities/error");
const Image = require("@app/entities/image");

class ApplicationInformation{
  // _monoId: string
  // _nickName: string
  // _profilePicture: Image

  /**
   * 
   * @param {string} monoId 
   * @param {string} nickName 
   */
  constructor(monoId, nickName){
    this.monoId = monoId;
    this.nickName = nickName;
  }

  get monoId(){ return this._monoId; }
  set monoId(value){ 
    if(!value) throw new CustomError("application-information/mono-id-not-valid", "Mono ID is not valid"); 
    if(!value.trim()) throw new CustomError("application-information/mono-id-not-valid", "Mono ID is not valid");
    if(isInvalidMonoId(value)) throw new CustomError("application-information/mono-id-not-valid", "Mono ID is not valid");
    this._monoId = value.toLowerCase().trim(); 
  }

  get nickName(){ return this._nickName; }
  set nickName(value){
    if(!value) throw new CustomError("application-information/nick-name-not-valid", "Nickname is not valid");
    if(!value.trim()) throw new CustomError("application-information/nick-name-not-valid", "Nickname is not valid");
    this._nickName = value;
  }

  get profilePicture(){ return this._profilePicture }
  set profilePicture(value){
    if(!(value instanceof Image)) throw new CustomError("profile-pic-not-valid", "Profile picture must be Image object");
    this._profilePicture = value
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

function isInvalidMonoId(monoId){
  const validMonoIdRegex = /[^a-zA-Z0-9_.]/gi;
  return validMonoIdRegex.test(monoId);
}
module.exports = ApplicationInformation;