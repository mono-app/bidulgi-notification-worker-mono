const CustomError = require("@app/entities/error");

class PersonalInformation {
  // _givenName: string
  // _familyName: string
  // _gender: string

  /**
   * 
   * @param {string} givenName 
   * @param {string} familyName 
   * @param {string} gender
   */
  constructor(givenName, familyName, gender){
    this.givenName = givenName
    this.familyName = familyName
    this.gender = gender
  }

  get givenName() { return this._givenName }
  set givenName(value) { 
    if(!value || !value.trim())  throw new CustomError("personal-information/given-name-not-valid", "Given Name is not valid");
    if(!isValidName(value)) throw new CustomError("personal-information/given-name-must-be-char", "Given Name must be character");
    this._givenName = value.trim()
  }

  get familyName() { return this._familyName }
  set familyName(value) { 
    if(!value || !value.trim()) throw new CustomError("personal-information/family-name-not-valid", "Family Name is not valid");
    if(!isValidName(value)) throw new CustomError("personal-information/family-name-must-be-char", "Family Name must be character");
    this._familyName = value.trim()
  }

  get gender() { return this._gender }
  set gender(value) { 
    if(!value || !value.trim()) throw new CustomError("personal-information/gender-not-valid", "Gender is not valid");
    if(!isValidName(value)) throw new CustomError("personal-information/gender-must-be-char", "Gender must be character");
    this._gender = value.trim()
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

function isValidName(value){
  const validRegex = /[^a-zA-Z ]/gi;
  return !validRegex.test(value);
}
module.exports = PersonalInformation;