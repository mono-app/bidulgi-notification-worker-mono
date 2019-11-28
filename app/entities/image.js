class Image{
  // downloadUrl: string
  // storagePath: string

  /**
   * 
   * @param {string} downloadUrl 
   * @param {string} storagePath 
   */
  constructor(downloadUrl, storagePath){
    this.downloadUrl = downloadUrl
    this.storagePath = storagePath
  }
}
module.exports = Image;