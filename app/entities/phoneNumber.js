const libphonenumber = require('libphonenumber-js');
const CustomError = require("@app/entities/error");

class PhoneNumber{
  // number: string
  // isVerified: boolean

  /**
   * 
   * @param {string} number 
   * @param {boolean} isVerified 
   */
  constructor(number, isVerified){
    this.number = normalizePhoneNumber(number, "ID", "62");
    this.isVerified = isVerified;
  }

}

function normalizePhoneNumber(number, countryCode, countryNumber){
  let result = number.toString()
  if(result.length <= 2) throw new CustomError("phone-number/not-valid", "Phone number is not valid");

  const phoneNumber1 = libphonenumber.parsePhoneNumberFromString(result, countryCode)
  if(phoneNumber1 && phoneNumber1.isPossible()){
    // check if there is `+` 
    if(result.substring(0,1) === "+") result = result.substr(1);
    if(!result.toLowerCase().match(/^[0-9]+$/)) throw new CustomError("phone-number/not-valid", "Phone number is not valid");

    // change 0 to countryNumber 
    if(result.substring(0,1) === "0"){
      result = result.substr(1);
      result = `${countryNumber}${result}`;
    }

    // if 2 first letter is not same with countryNumber, add the countryNumber at the beginning
    if(result.substring(0,2) !== countryNumber) result = `${countryNumber}${result}`;
    const phoneNumber2 = libphonenumber.parsePhoneNumberFromString(`+${result}`, countryCode);
    if(phoneNumber2.isValid()) return result
  }else throw new CustomError("phone-number/not-valid", "Phone number is not valid");
}
module.exports = PhoneNumber;