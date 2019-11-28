const CustomError = require("@app/entities/error");
const Email = require("@app/entities/email");
const PhoneNumber = require("@app/entities/phoneNumber");
const ApplicationInformation = require("@app/entities/applicationInformation");
const PersonalInformation = require("@app/entities/personalInformation");
const Password = require("@app/entities/password");

class User{
  // _id: string;
  // _password: Password;
  // id: string;
  // isCompleteSetup: boolean;
  // phoneNumber: PhoneNumber
  // personalInformation: PersonalInformation
  // applicationInformation: ApplicationInformation

  /**
   * 
   * @param {string} id 
   * @param {string} email 
   * @param {boolean} isCompleteSetup 
   */
  constructor(id, email, isCompleteSetup){
    this.id = id? id: null;
    this.isCompleteSetup = isCompleteSetup? isCompleteSetup: false;
    this.phoneNumber = null;
    this._email = email? new Email(email): null;
    this._password = null;
  }

  get id() { return this._id }
  set id(value) {
    if(!value) throw new CustomError("null-value","user id cannot empty")
    this._id = value
  }

  /**
   * 
   * @param {DocumentSnapshot} documentSnapshot 
   */
  static fromSnapshot(documentSnapshot){
    const data = documentSnapshot.data();
    const user = new User(documentSnapshot.id, data.email, data.isCompleteSetup)
    
    user.creationTime = data.creationTime
    try{
      user.phoneNumber = new PhoneNumber(data.phoneNumber.value, data.phoneNumber.isVerified) 
    }catch(err){}

    if(data.isCompleteSetup){
      const monoId = data.applicationInformation.monoId
      const nickName = data.applicationInformation.nickName
      const applicationInformation = new ApplicationInformation(monoId, nickName)
      try{
        const downloadUrl = data.applicationInformation.profilePicture.downloadUrl
        const storagePath = data.applicationInformation.profilePicture.storagePath
        applicationInformation.profilePicture = new Image(downloadUrl, storagePath)
        user.profilePicture = downloadUrl
      }catch(err){
        user.profilePicture = ""
      }
      user.applicationInformation = applicationInformation
      const givenName = data.personalInformation.givenName
      const familyName = data.personalInformation.familyName
      const gender = data.personalInformation.gender
      user.personalInformation = new PersonalInformation(givenName, familyName, gender)
      user.location = data.location
      user.lastOnline = data.lastOnline
      user.tokenInformation = data.tokenInformation
    }
    return user;
  }

  get email(){ return this._email.address }
  set email(value){ this._email = new Email(value) }

  get password(){ return this._password.value }
  set password(value){ this._password = new Password(value) }
  
  get data(){
    const transformedData = {}
    Object.keys(this).map((prop) => {
      const newProp = (prop.indexOf("_") === 0)? prop.substring(1): prop;
      transformedData[newProp] = this[prop];
    })
    return transformedData;
  }
}
module.exports = User;