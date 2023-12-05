const mongoose = require("mongoose");

const logSchema = new mongoose.Schema({
    date: {type: Number, required: true},
    module: {type: String, required: true},
    details: {type: [String], default:[]},
    type: {type: String, enum:['info','success','error','warning'],required: true},
    subType: {type: String, enum:['database','client','server','general'],required: true},
    host: {type: String,required: true},

},
    {
        versionKey: false,
        timeStamp: true
    });

    const Logs = mongoose.model('logs',logSchema);

    module.exports = Logs;