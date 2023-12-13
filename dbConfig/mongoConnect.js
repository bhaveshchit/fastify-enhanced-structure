require('dotenv').config();
const mongoose = require("mongoose");

class Database {
    constructor(){
        this.connect = null,
        this.connect();
    }
    async connect(){
        if(!this.connection){
            try {
                let url = process.env.MONGO_URL;
                if (typeof url == "undefined"){
                    url = `mongodb+srv://${process.env.MONGO_USERNAME}: ${process.env.MONGO_PASSWORD}@${process.env.MONGO_HOST}/${process.env.MONGO_DATABASE}?retryWrites=true&w=majority`
                }
                await mongoose.connect(url);
                this.connection = mongoose.connection;
                console.log("MongoDb connected successfully")
            } catch (error) {
                console.log("MongoError ************ ",error)    
            }
        }
        return this.connection;
    }
}

module.exports = new Database();