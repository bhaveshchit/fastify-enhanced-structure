const {errorCode} = require("../constants/statusCode");
const {unknownError}= require("../constants/statusText");

class ServerError extends Error{
    constructor(msg,data){
        super(msg || unknownError);
        this.name = "ServerError";
        this.statusCode = errorCode
        this.data = data || null;
    }
}

module.exports = ServerError;