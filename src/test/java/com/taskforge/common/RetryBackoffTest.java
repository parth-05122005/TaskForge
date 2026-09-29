package com.taskforge.common;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class RetryBackoffTest {
    @Test void appliesFullJitterWithinExponentialCeiling() {
        for(int i=0;i<100;i++) {
            assertTrue(RetryBackoff.seconds(1,1,20)>=1);
            assertTrue(RetryBackoff.seconds(1,1,20)<=2);
            assertTrue(RetryBackoff.seconds(2,1,20)>=2);
            assertTrue(RetryBackoff.seconds(2,1,20)<=4);
            assertTrue(RetryBackoff.seconds(6,1,20)>=10);
            assertTrue(RetryBackoff.seconds(6,1,20)<=20);
        }
        assertEquals(20,RetryBackoff.seconds(6,20,20));
    }

    @Test void rejectsInvalidRetrySettings() {
        assertThrows(IllegalArgumentException.class,()->RetryBackoff.seconds(0,2,20));
        assertThrows(IllegalArgumentException.class,()->RetryBackoff.seconds(1,4,3));
    }
}
