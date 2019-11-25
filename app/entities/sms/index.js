const CustomError = require("@app/entities/error");

class Sms{
// provider: string
// apiKey: string
// apiSecret: string
// senderId: string

  /**
   * 
   * @param {string} provider 
   * @param {string} apiKey 
   * @param {string} apiSecret 
   * @param {string} senderId 
   */
  constructor(provider="nexmo", apiKey, apiSecret, senderId="Mono"){
    this.provider = provider
    this.apiKey = apiKey
    this.apiSecret = apiSecret
    this.senderId = senderId
  }
  
  get provider() { return this._provider }
  set provider(value) { 
    if(!value) throw new CustomError("null-value","sms provider cannot be empty")
    this._provider = value
  }

  get apiKey() { return this._apiKey }
  set apiKey(value) { 
    if(!value) throw new CustomError("null-value","sms apiKey cannot be empty")
    this._apiKey = value
  }

  get apiSecret() { return this._apiSecret }
  set apiSecret(value) { 
    if(!value) throw new CustomError("null-value","sms apiSecret cannot be empty")
    this._apiSecret = value
  }

  get senderId() { return this._senderId }
  set senderId(value) { 
    if(!value) throw new CustomError("null-value","sms senderId cannot be empty")
    this._senderId = value
  }
}
module.exports = Sms;