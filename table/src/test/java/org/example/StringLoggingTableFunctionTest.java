package org.example;

import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.mockito.Mockito.*;

public class StringLoggingTableFunctionTest {

    private StringLoggingTableFunction loggingFunction;
    private Collector<Row> collector;

    @BeforeEach
    @SuppressWarnings("unchecked")
    public void setUp() {
        loggingFunction = new StringLoggingTableFunction();
        collector = Mockito.mock(Collector.class);
        loggingFunction.setCollector(collector);
    }

    @Test
    public void testEval() {
        String testString = "hello world";
        loggingFunction.eval(testString);

        // Verify that collect was called once with a Row containing the input string
        verify(collector, times(1)).collect(Row.of(testString));
    }

    @Test
    public void testEvalWithEmptyString() {
        loggingFunction.eval("");

        // Verify that collect was never called for an empty string
        verify(collector, never()).collect(any());
    }

    @Test
    public void testEvalWithNull() {
        loggingFunction.eval(null);

        // Verify that collect was never called for a null string
        verify(collector, never()).collect(any());
    }
}
