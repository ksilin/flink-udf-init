package org.example;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TshirtSizingIsSmallerTest {

    private final TshirtSizingIsSmaller tshirtSizingIsSmaller = new TshirtSizingIsSmaller();

    @Test
    void testSmallerSize() {
        assertTrue(tshirtSizingIsSmaller.eval("Small", "Medium"));
        assertTrue(tshirtSizingIsSmaller.eval("XS", "S"));
    }

    @Test
    void testLargerSize() {
        assertFalse(tshirtSizingIsSmaller.eval("Large", "Medium"));
        assertFalse(tshirtSizingIsSmaller.eval("XL", "L"));
    }

    @Test
    void testEqualSize() {
        assertFalse(tshirtSizingIsSmaller.eval("Medium", "Medium"));
        assertFalse(tshirtSizingIsSmaller.eval("S", "S"));
    }

    @Test
    void testInvalidSize() {
        assertFalse(tshirtSizingIsSmaller.eval("Invalid", "Medium"));
        assertFalse(tshirtSizingIsSmaller.eval("Small", "Invalid"));
        assertFalse(tshirtSizingIsSmaller.eval("Invalid", "Invalid"));
    }
}
