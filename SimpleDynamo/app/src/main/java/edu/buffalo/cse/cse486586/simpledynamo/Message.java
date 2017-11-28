package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.Serializable;

/**
 * Object of this class is used by the client to communicate with server
 */

public class Message implements Serializable {
    int sender;
    String requestType;
    String key;
    String value;
    public Message(String requestType, String key, String value){
        this.requestType = requestType;
        this.key = key;
        this.value = value;
    }
    public Message(String requestType, int sender){
        this.sender = sender;
        this.requestType = requestType;
    }

    public boolean isRequest(String request){
        return this.requestType.equals(request);
    }
    public int getSender(){
        return this.sender;
    }
    public String getKey(){
        return this.key;
    }
    public  String getValue(){
        return this.value;
    }
}
