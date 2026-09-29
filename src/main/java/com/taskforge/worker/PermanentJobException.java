package com.taskforge.worker;

/** A handler failure that should not be retried, even when attempts remain. */
public class PermanentJobException extends RuntimeException {
    public PermanentJobException(String message) { super(message); }
    public PermanentJobException(String message, Throwable cause) { super(message, cause); }
}
