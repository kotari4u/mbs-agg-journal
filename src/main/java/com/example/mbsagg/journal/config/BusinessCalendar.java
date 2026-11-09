package com.example.mbsagg.journal.config;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;

import org.springframework.stereotype.Component;

/**
 * The business date: a calendar day in {@code mbsagg.zone}, starting at {@code mbsagg.rollover}. Dates are ISO
 * strings ({@code 2026-11-09}), which sort correctly as strings.
 */
@Component
public class BusinessCalendar {
    private final Clock clock;
    private final ZoneId zone;
    private final LocalTime rollover;

    public BusinessCalendar(Clock clock, MbsAggProperties properties) {
        this.clock = clock;
        this.zone = properties.zone();
        this.rollover = properties.rollover();
    }

    /** The business date a record with this timestamp belongs to. */
    public String businessDate(long epochMillis) {
        LocalDateTime t = LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMillis), zone);
        LocalDate d = t.toLocalDate();
        if (!rollover.equals(LocalTime.MIDNIGHT) && !t.toLocalTime().isBefore(rollover)) d = d.plusDays(1);
        return d.toString();
    }

    public String today() {
        return businessDate(clock.millis());
    }

    /** When {@code bizDate} starts, {@code margin} early. */
    public Instant startOf(String bizDate, Duration margin) {
        LocalDate d = LocalDate.parse(bizDate);
        LocalDate startDay = rollover.equals(LocalTime.MIDNIGHT) ? d : d.minusDays(1);
        return startDay.atTime(rollover).atZone(zone).toInstant().minus(margin);
    }
}
