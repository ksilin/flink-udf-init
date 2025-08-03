package org.example;

import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TextExtractorFunctionTest {

    private TextExtractorFunction textExtractorFunction;
    private Collector<Row> collector;

    @BeforeEach
    public void setUp() {
        textExtractorFunction = new TextExtractorFunction();
        collector = Mockito.mock(Collector.class);
        textExtractorFunction.setCollector(collector);
    }

    @Test
    public void testEval() {
        List<Row> results = new ArrayList<>();
        Mockito.doAnswer(invocation -> {
            Row row = invocation.getArgument(0);
            results.add(row);
            return null;
        }).when(collector).collect(Mockito.any(Row.class));

        textExtractorFunction.eval("hello world 123");

        assertEquals(3, results.size());
        assertEquals(Row.of("hello", 5), results.get(0));
        assertEquals(Row.of("world", 5), results.get(1));
        assertEquals(Row.of("123", 3), results.get(2));
    }

    @Test
    public void testEvalWithSpecialCharacters() {
        List<Row> results = new ArrayList<>();
        Mockito.doAnswer(invocation -> {
            Row row = invocation.getArgument(0);
            results.add(row);
            return null;
        }).when(collector).collect(Mockito.any(Row.class));

        textExtractorFunction.eval("hello, world! 123");

        assertEquals(3, results.size());
        assertEquals(Row.of("hello", 5), results.get(0));
        assertEquals(Row.of("world", 5), results.get(1));
        assertEquals(Row.of("123", 3), results.get(2));
    }

    @Test
    public void testEvalWithEmptyString() {
        List<Row> results = new ArrayList<>();
        Mockito.doAnswer(invocation -> {
            Row row = invocation.getArgument(0);
            results.add(row);
            return null;
        }).when(collector).collect(Mockito.any(Row.class));

        textExtractorFunction.eval("");

        assertEquals(0, results.size());
    }

    @Test
    public void testEvalWithCustomRegex() {
        List<Row> results = new ArrayList<>();
        Mockito.doAnswer(invocation -> {
            Row row = invocation.getArgument(0);
            results.add(row);
            return null;
        }).when(collector).collect(Mockito.any(Row.class));

        textExtractorFunction.eval("apple,banana,cherry", ",");

        assertEquals(3, results.size());
        assertEquals(Row.of("apple", 5), results.get(0));
        assertEquals(Row.of("banana", 6), results.get(1));
        assertEquals(Row.of("cherry", 6), results.get(2));
    }

    @Test
    public void testEvalWithMultiCharRegex() {
        List<Row> results = new ArrayList<>();
        Mockito.doAnswer(invocation -> {
            Row row = invocation.getArgument(0);
            results.add(row);
            return null;
        }).when(collector).collect(Mockito.any(Row.class));

        textExtractorFunction.eval("one##two##three", "##");

        assertEquals(3, results.size());
        assertEquals(Row.of("one", 3), results.get(0));
        assertEquals(Row.of("two", 3), results.get(1));
        assertEquals(Row.of("three", 5), results.get(2));
    }
}