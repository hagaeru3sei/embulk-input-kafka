package data.column;

import org.embulk.input.kafka.data.column.ColumnType;
import org.embulk.input.kafka.exception.ColumnTypeNotFoundException;
import org.junit.Test;

public class TestColumnType
{
    @Test(expected = ColumnTypeNotFoundException.class)
    public void InvalidColumnTypeTest() throws ColumnTypeNotFoundException {
        ColumnType.get("tinyint");
    }
}
