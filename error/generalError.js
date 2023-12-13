const {errorCode} = require("../constants/statusCode");
const {error}= require("../constants/statusText");

class GeneralError extends Error{
    constructor(msg,data){
        super(msg || error);
        this.name = "GeneralError";
        this.statusCode = errorCode
        this.data = data || null;
    }
}

module.exports = GeneralError;