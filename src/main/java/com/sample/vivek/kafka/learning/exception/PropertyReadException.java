package com.sample.vivek.kafka.learning.exception;

/**
 * @author : Vivek Kumar Gupta
 * @since : 20/07/20
 */
public class PropertyReadException extends Exception {

    public PropertyReadException(String msg){
        super(msg);
    }

    public PropertyReadException(String msg, Throwable exp){
        super(msg,exp);
    }



}