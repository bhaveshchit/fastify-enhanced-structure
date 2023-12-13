require('dotenv').config();
const mysql =  require('mysql2/promise');
const projectConfig = require("../projectConfig.json");
const {Operations} = require("../constants/database");
const DatabaseError = require("../error/databaseError");

class Database {
    constructor(){
        this.pool = mysql.createPool({
            connectionLimit: process.env.MYSQL_CONNECTION_LIMIT || 5,
            host: process.MYSQL_HOST,
            user : process.env.MYSQL_USERNAME,
            password: process.env.MYSQL_PASSWORD,
            database: process.env.MYSQL_DB_NAME,
            port: process.env.MYSQL_DB_PORT,
            connectionTimeout: 10000,
        });
        this.connect();
    }

    static async getStandAloneConnection(){
        const pool = mysql.createPool({
            connectionLimit: 1,
            host: process.MYSQL_HOST,
            user : process.env.MYSQL_USERNAME,
            password: process.env.MYSQL_PASSWORD,
            database: process.env.MYSQL_DB_NAME,
            port: process.env.MYSQL_DB_PORT,
            connectionTimeout: 10000,
        });

        const connection = await pool.getConnection();
        return connection;
    }

    async connect(){
        try {
            const connection = await this.pool.getConnection();
            console.log('Database connected successfully');
            connection.release()
        } catch (error) {
            console.log("MYsql Error ********** ", error)
            throw error;
        }
    }

    async query(statement) {
        try {
            let operation = statement?.operation || Operations.SELECT;
            if(projectConfig?.db?.logs || statement?.logging){
                console.log('Sql ====> ', getRaw(statement));
            }

            const [rows, fields] = await this.pool.execute(statement.text,statement.values);

            if(projectConfig?.db?.printResult || statement?.printResult) {
                console.log([rows,fields]);
            }
            return statement?.rowsOnly ? {rows}:{rows,fields};
        } catch (error) {
            throw new DatabaseError(error);
        }
    }

    static getInstance(){
        if(!this.instance) {
            this.instance = new Database();
        }
        return this.instance;
    }
}

function getRaw(statement) {
    let text = statement.text;
    for (let value of statement.values){
        if(typeof value == 'number') {
            text = text.replace ("?",`'${value}`);
        }else{
            text = text.replace("?", `'${value}`);
        }
    }
    return text;
}

function inMapper(array){
    if(typeof array == 'undefined' || array.length < 1){
        throw new DatabaseError("invalid array passed to inMapper")
    }
    let value = array?.map((value)=>`'${value}`).join(', ');
    return value;
}

function typeTransformer(data){
    let result;
    if(typeof data== 'number' || typeof data == 'string'){
        result = data;
    }
    else if(typeof data == 'object') {
        result = `${JSON.stringify(data)}`
    }else{
        result = `'${data}'`
    }
    return result;
}

function transformValues(array){
    const result = [];
    for (let a of array ){
        const keys = object.keys(a);
        for (let k of keys ){
            if(!(a[k] && a[k].toString().startsWith('POINT')))
            result.push(typeTransformer(a[k]));
        }
    }
    return result;
}

function removeUndefinedAndNull(array){
    if(array.constructor === object){
        const obj = array;
        const keys = object.keys(obj);
        for (let k of keys){
            if(typeof obj[k] == 'undefined' || obj[k] == null) {
                delete obj[k];
            }
        }
        return obj;
    } else {
        const result =[];
        for (let element of array){
            const obj = element;
            const keys = object.keys(obj);
            for (let k of keys){
                if(typeof obj[k] == 'undefined' || obj[k] == null) {
                    delete obj[k];
                }
            }
            result.push(obj);            
        }
        return result;
    }
}


function validateValues(array){
    let result = [];
    for (let a of array){
        result.push(Object.keys(a).length);
    }
    result = [...new Set(result)]
    return result.length > 1 ? false:true;
}

function getColumnReplacement(columns){
    let columnReplacement = '';
    let columnValues = [];
    const keys = Object.keys(columns);
    for (let key of keys){
        if (columns[key].toString().trim()== 'now()' || columns[key].toString().trim() == 'CURRENT_TIMESTAMP'){
            columnReplacement += `${key} = now(),`
        }
        else if(columns[key].toString().startsWith("POINT")){
            columnReplacement += `${key} = ${columns[key].replace(/"/g, '')},`
        }else {
            const value = typeTransformer(column[key]);
            columnReplacement += ` ${key} = ?,`
            columnValues.push(value);
        }
    }
    columnReplacement += `updated_at = now()`
    return {
        columnReplacement,
        columnValues
    };
}

function insertData(tableName, dataArray) {
    if (dataArray.length === 0) {
        throw new DatabaseError('No data to insert');
    }

    if (!Array.isArray(dataArray)) {
        dataArray = [dataArray];
    }

    dataArray = removeUndefinedAndNull(dataArray);

    if (!validateValues(dataArray)) {
        throw new DatabaseError('Input values is distorted');
    }

    const columns = Object.keys(dataArray[0]);

    let firstObject = dataArray[0]

    let placeHolderLength = columns.length;
    if (firstObject['coordinates'] && firstObject['coordinates'].toString().startsWith("POINT")) {
        placeHolderLength = columns.length - 1;
    }

    const placeholders = Array.from({ length: placeHolderLength }, () => '?').join(', ');
    const values = transformValues(dataArray);
    const text = `INSERT INTO ${tableName} (${columns.join(', ')}) VALUES ${dataArray
        .map((item) => {
            dataArray.push(...columns.map((col) => item[col]));
            if (item['coordinates'] && item['coordinates'].toString().startsWith("POINT")) {
                let coordinateData = item['coordinates'].replace(/"([^"]+(?="))"/g, '$1')
                return `(${placeholders}, ${coordinateData})`
            } else
                return `(${placeholders});`
        })
        .join(', ')};`;

    const statement = {
        text,
        values
    }
    return statement;
}


function updateSingle(table,columns,id){
    columns = removeUndefinedAndNull(columns);
    let {columnReplacement, columnValues} = getColumnReplacement(columns);
    let text = `update ${table} set ${columnReplacement} where id = ?;`
    const statement = {
        text,
        values: [...columnValues,...[id]]
    }
    return statement;
}


module.exports = {
    db:Database.getInstance(),
    dbConnection: async()=> await Database.getStandAloneConnection(),
    inMapper,
    insertData,
    updateSingle,
    getColumnReplacement
}