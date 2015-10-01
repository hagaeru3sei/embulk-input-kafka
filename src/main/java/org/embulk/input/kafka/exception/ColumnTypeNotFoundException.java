package org.embulk.input.kafka.exception;

public class ColumnTypeNotFoundException extends Exception
{
    public ColumnTypeNotFoundException(String message)
    {
        super(message);
    }
}
