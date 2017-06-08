'use strict'; 

var amqplib = require('amqplib'); 


/**
 * Class to handle mqueue based communication
 */
const MQueue = module.exports = function (){

    this.mqBrokerURL    = 'amqp://djowkmft:w24tKb-uLuToqkXUr4FWeiHVooshZYaT@penguin.rmq.cloudamqp.com/djowkmft';     // Broker address 
    this.amqplib        = amqplib;  // reference to amqplib module
     
    this.channel        = null;     // channel to be used in producer and consumer communication pattern
    this.queue          = "myqueue";     // queue to be used for producer , consmuer communication 

    this.initConnection() ; 

}




/**
 * Initialize the connection toward Message broker
 * We use a Rabbit MQ broker which support amqp protocoll  
 */
MQueue.prototype.initConnection = function () {

    this.mqBrokerURL    = 'amqp://djowkmft:w24tKb-uLuToqkXUr4FWeiHVooshZYaT@penguin.rmq.cloudamqp.com/djowkmft';
    const self = this; 

    this.open = amqplib.connect( self.mqBrokerURL).then ( function (conn) {
        let ok = conn.createChannel(); 
        ok.then( function (ch ) {
            self.channel = ch; // save the channel for further communicaton 
            console.log ('[AMQP] connection and channel created ')
        }); 

    }).then(null, console.warn); 
}
    
   
 
/**
 * General method to send to a predefined queue 
 * 
 */
MQueue.prototype.sendByMessageQueue = function (msg) {

    //if (this.channel != null) {

    const self = this; 
    this.channel.assertQueue(this.queue, {durable: false}); // check if te queue already exists
    this.channel.sendToQueue(this.queue, new Buffer(msg)); // send it by the queue 
    console.log("[AMQP] sent it by the producer the message %s", msg ); 
    //}
}

/**
 * General method (callbackk)  to receive any message coming fro the queue  
 */
MQueue.prototype.receiveByMessageQueue = function () {
    //if ( this.channel != null) { 
    const self = this; 
    this.channel.assertQueue(this.queue, {durable: false}); // check if te queue already exists
    this.channel.consume(this.queue, function (msg){

        console.log("[AMQP] Received %s", msg.content.toString()); 

    }, {noAck:true}); 
    //}
}



