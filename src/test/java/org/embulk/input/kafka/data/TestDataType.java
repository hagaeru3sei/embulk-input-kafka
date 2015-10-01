package data;

import org.embulk.input.kafka.data.DataType;
import org.embulk.input.kafka.exception.DataTypeNotFoundException;
import org.junit.Test;

public class TestDataType
{
    @Test(expected = DataTypeNotFoundException.class)
    public void InvalidDataTypeNameTest() throws DataTypeNotFoundException {
        DataType.get("binary");
    }
}
