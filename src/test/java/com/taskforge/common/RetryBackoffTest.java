package com.taskforge.common;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class RetryBackoffTest {
    @Test void growsExponentiallyAndCapsAtConfiguredMaximum() {
        assertEquals(2,RetryBackoff.seconds(1,1,20));
        assertEquals(4,RetryBackoff.seconds(2,1,20));
        assertEquals(20,RetryBackoff.seconds(6,1,20));
    }

    @Test void rejectsInvalidRetrySettings() {
        assertThrows(IllegalArgumentException.class,()->RetryBackoff.seconds(0,2,20));
        assertThrows(IllegalArgumentException.class,()->RetryBackoff.seconds(1,4,3));
    }
}
