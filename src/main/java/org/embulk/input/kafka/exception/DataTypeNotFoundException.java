package org.embulk.input.kafka.exception;

public class DataTypeNotFoundException extends Exception
{
    public DataTypeNotFoundException(String message)
    {
        super(message);
    }
}
