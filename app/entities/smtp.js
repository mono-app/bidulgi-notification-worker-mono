const CustomError = require("@app/entities/error");

class Smtp{
// _host: string
// _port: string
// _username: string
// _password: string

  /**
   * 
   * @param {string} host 
   * @param {string} port 
   * @param {string} username 
   * @param {string} password 
   */
  constructor(host, port, username, password){
    this.host = host
    this.port = port
    this.username = username
    this.password = password
  }

  get host() { return this._host }
  set host(value) { 
    if(!value) throw new CustomError("null-value","smtp host cannot be empty")
    this._host = value
  }

  get port() { return this._port }
  set port(value) { 
    if(!value) throw new CustomError("null-value","smtp port cannot be empty")
    this._port = value
  }

  get username() { return this._username }
  set username(value) { 
    if(!value) throw new CustomError("null-value","smtp username cannot be empty")
    this._username = value
  }

  get password() { return this._password }
  set password(value) { 
    if(!value) throw new CustomError("null-value","smtp password cannot be empty")
    this._password = value
  }

}
module.exports = Smtp;