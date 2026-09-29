package com.taskforge.worker;

/** A handler failure that may succeed when the platform retries the attempt. */
public class RetryableJobException extends RuntimeException {
    public RetryableJobException(String message) { super(message); }
    public RetryableJobException(String message, Throwable cause) { super(message, cause); }
}
