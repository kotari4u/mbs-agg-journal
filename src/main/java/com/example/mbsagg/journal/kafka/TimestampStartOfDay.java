package com.example.mbsagg.journal.kafka;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Component;

import com.example.mbsagg.journal.config.BusinessCalendar;
import com.example.mbsagg.journal.config.MbsAggProperties;

/**
 * Finds the start of a business day with {@code offsetsForTimes}, {@code mbsagg.aggregator.start-of-day-margin}
 * earlier. Records from the previous day that fall in the margin are skipped by the worker (their business date is
 * older). Works best with the journal topic on {@code message.timestamp.type=LogAppendTime}.
 */
@Component
public class TimestampStartOfDay implements StartOfDay {
    private final BusinessCalendar calendar;
    private final Duration margin;

    public TimestampStartOfDay(BusinessCalendar calendar, MbsAggProperties properties) {
        this.calendar = calendar;
        this.margin = properties.aggregator().startOfDayMargin();
    }

    @Override
    public long offset(Consumer<?, ?> consumer, TopicPartition partition, String bizDate) {
        long startMillis = calendar.startOf(bizDate, margin).toEpochMilli();
        Map<TopicPartition, OffsetAndTimestamp> r = consumer.offsetsForTimes(Map.of(partition, startMillis));
        OffsetAndTimestamp found = r.get(partition);
        if (found != null) return found.offset();
        return consumer.endOffsets(List.of(partition)).get(partition);   // nothing that late yet: start at the end
    }
}
