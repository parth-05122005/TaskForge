package com.taskforge.job;

import org.junit.jupiter.api.Test;
import java.time.*;
import static org.junit.jupiter.api.Assertions.*;

class CronSupportTest {
    @Test void acceptsFiveFieldCronAndReturnsFutureInstant(){
        Instant from=Instant.parse("2026-09-29T12:34:30Z");
        Instant next=CronSupport.next("35 12 * * *","UTC",from);
        assertEquals(Instant.parse("2026-09-29T12:35:00Z"),next);
    }
    @Test void calculatesAccordingToConfiguredZone(){
        Instant from=Instant.parse("2026-09-29T00:00:00Z");
        Instant next=CronSupport.next("0 9 * * *","Asia/Kolkata",from);
        assertEquals(Instant.parse("2026-09-29T03:30:00Z"),next);
    }
    @Test void skipsMissedCronOccurrenceAndSelectsNextFutureRun(){
        Instant next=CronSupport.next("0 9 * * *","Asia/Kolkata",Instant.parse("2026-09-29T10:00:00Z"));
        assertEquals(Instant.parse("2026-09-30T03:30:00Z"),next);
    }
    @Test void skipsLocalTimeThatDoesNotExistDuringSpringForward(){
        Instant next=CronSupport.next("0 30 2 * * *","America/New_York",Instant.parse("2026-03-08T05:00:00Z"));
        assertEquals(Instant.parse("2026-03-09T06:30:00Z"),next);
    }
    @Test void runsRepeatedLocalTimeAgainDuringFallBack(){
        Instant next=CronSupport.next("0 30 1 * * *","America/New_York",Instant.parse("2026-11-01T05:30:01Z"));
        assertEquals(Instant.parse("2026-11-01T06:30:00Z"),next);
    }
    @Test void rejectsInvalidCronAndTimezone(){
        assertThrows(IllegalArgumentException.class,()->CronSupport.normalize("61 * * * *"));
        assertThrows(IllegalArgumentException.class,()->CronSupport.next("0 0 * * *","Nowhere/Invalid",Instant.now()));
    }
}
