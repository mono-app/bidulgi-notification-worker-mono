class CustomError extends Error{
  // name: string
  // message: string
  // code: string
  /**
   * 
   * @param {string} name 
   * @param {string} message 
   */
  constructor(name, message){
    super();
    this.name = name;
    this.code = name;
    this.message = message;
  }

  toErrorResponse(){
    return { error: {code: this.code, message: this.message} }
  }
}
module.exports = CustomError;