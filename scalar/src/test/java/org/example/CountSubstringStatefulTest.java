package org.example;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class CountSubstringStatefulTest {

    private CountSubstringStateful countSubstring;
    private CountSubstringStateful.CountAccumulator accumulator;

    @BeforeEach
    public void setUp() {
        countSubstring = new CountSubstringStateful();
        accumulator = countSubstring.createAccumulator();
    }

    @Test
    public void testCreateAccumulator() {
        assertEquals(0L, accumulator.value);
    }

    @Test
    public void testAccumulate() {
        countSubstring.accumulate(accumulator, "hello world hello", "hello");
        assertEquals(2L, accumulator.value);

        countSubstring.accumulate(accumulator, "another hello", "hello");
        assertEquals(3L, accumulator.value);
    }

    @Test
    public void testAccumulateNoMatch() {
        countSubstring.accumulate(accumulator, "this is a test", "world");
        assertEquals(0L, accumulator.value);
    }

    @Test
    public void testAccumulateWithEmptyStrings() {
        countSubstring.accumulate(accumulator, "", "a");
        assertEquals(0L, accumulator.value);

        countSubstring.accumulate(accumulator, "abc", "");
        assertEquals(0L, accumulator.value);
    }

    @Test
    public void testAccumulateWithNulls() {
        countSubstring.accumulate(accumulator, null, "a");
        assertEquals(0L, accumulator.value);

        countSubstring.accumulate(accumulator, "abc", null);
        assertEquals(0L, accumulator.value);
    }

    @Test
    public void testRetract() {
        countSubstring.accumulate(accumulator, "hello hello hello", "hello"); // acc = 3
        countSubstring.retract(accumulator, "hello", "hello"); // retract 1
        assertEquals(2L, accumulator.value);

        countSubstring.retract(accumulator, "nothing", "hello"); // retract 0
        assertEquals(2L, accumulator.value);

        countSubstring.retract(accumulator, "hello again hello", "hello"); // retract 2
        assertEquals(0L, accumulator.value);
    }

    @Test
    public void testMerge() {
        CountSubstringStateful.CountAccumulator acc1 = new CountSubstringStateful.CountAccumulator(5L);
        CountSubstringStateful.CountAccumulator acc2 = new CountSubstringStateful.CountAccumulator(10L);
        CountSubstringStateful.CountAccumulator acc3 = new CountSubstringStateful.CountAccumulator(2L);

        countSubstring.merge(accumulator, Arrays.asList(acc1, acc2, acc3));
        assertEquals(17L, accumulator.value);
    }

    @Test
    public void testMergeWithEmptyIterable() {
        countSubstring.merge(accumulator, Collections.emptyList());
        assertEquals(0L, accumulator.value);
    }

    @Test
    public void testGetValue() {
        accumulator.value = 123L;
        assertEquals(123L, countSubstring.getValue(accumulator));
    }

    @Test
    public void testFullCycle() {
        // Accumulate
        countSubstring.accumulate(accumulator, "one two one", "one"); // 2
        countSubstring.accumulate(accumulator, "one three", "one");   // 1
        assertEquals(3L, countSubstring.getValue(accumulator));

        // Retract
        countSubstring.retract(accumulator, "one two one", "one"); // -2
        assertEquals(1L, countSubstring.getValue(accumulator));

        // Merge
        CountSubstringStateful.CountAccumulator otherAcc = new CountSubstringStateful.CountAccumulator(5L);
        countSubstring.merge(accumulator, Collections.singletonList(otherAcc));
        assertEquals(6L, countSubstring.getValue(accumulator));
    }
}
