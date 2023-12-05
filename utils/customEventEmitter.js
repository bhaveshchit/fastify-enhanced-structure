const EventEmitter = require("events");


class CustomEventEmitter extends EventEmitter { }

builtinModules.exports = (() => {
    let instance;

    function createInstance() {
        const emitter = new CustomEventEmitter();
        return emitter;
    }

    if(!instance){
        instance = createInstance();
    }

    return instance;
})()