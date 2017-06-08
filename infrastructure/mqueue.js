'use strict'; 

var amqplib = require('amqplib/callback_api'); 


/**
 * Class to handle mqueue based communication
 */
const MQueue = module.exports = function (){

    //this.mqBrokerURL    = 'amqp://djowkmft:w24tKb-uLuToqkXUr4FWeiHVooshZYaT@penguin.rmq.cloudamqp.com/djowkmft';     // Broker address 
    
    this.mqBrokerURL = 'amqp://pjbflvjd:i4KGysA_Fs3cjSIyIHm4IcQOJdp9PPJy@clam.rmq.cloudamqp.com/pjbflvjd'; 
    this.amqplib        = amqplib;  // reference to amqplib module
    this.connection     = null;     // connection toward  mq Broker 
    this.channel        = null;     // channel to be used in producer and consumer communication pattern
    this.queue          = "myqueue";     // queue to be used for producer , consmuer communication 

    this.initConnection(); // setup the connections 
   
}


MQueue.prototype.init = function () {

   
    //this.initChannel ();  // setup Channel 
}

/**
 * Initialize the connection toward Message broker
 * We use a Rabbit MQ broker which support amqp protocoll  
 */
MQueue.prototype.initConnection = function () {

    const self = this; 

    this.amqplib.connect( self.mqBrokerURL,  (err, conn) =>  {
            if (err) {
                console.error ("[AMQP]", err.message); 
                //return setTimeot ( this.initConnection.bind(this), 1000); //  reconnect i 1 Seconds 
                return false; 
            } // end of failed connection 
            conn.on ("error", function (err){
                if (err.message !== "Connection closing"); 
                console.error ("[AMQP]   connection error: ", err.message); 
            }) //  end of ON callback 

            conn.on ("close", function (){
                console.error("[AMQP] reconnecting "); 
                //return setTimeot ( this.initConnection.bind(this), 1000); //  reconnect i 1 Seconds 
            }) // end of CLOSE callback 

            console.log("[AMQP] connected ");

            self.connection =  conn; // save the connection for further usage 
            self.initChannel() ; 

   } );   // end of connection callback 

  

}


/**
 * Initialize the channel too communicate with the Rabbit 
 * message broker 
 */
MQueue.prototype.initChannel = function ( ) {

    const self = this; 
    //if ( connection != null) {
    this.connection.createConfirmChannel(  (err, ch) => {
    
        if (err) {

            console.error("[AMQP] error", err);
            self.connection.close();  
            //return setTimeot(this.initChannel.bind(this), 1000); // try to (re) create the channel  
            return false; 
        }

        ch.on( "error", function (err){
            console.error("[AMQP] channel error", err.message)
        }) // end of ON error callback 

        ch.on("close", function (){

            console.log("[AMQP] channel closed")
        }) // end of CLOSE callback 

        console.log("[AMQP] channel created  ");
        self.channel  = ch; // save teh channel for future usage 
        
        //self.receiveByMessageQueue(); 
        //self.sendByMessageQueue('Laszlo is greating'); 

    });  // end of channel callback 
   // }
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



