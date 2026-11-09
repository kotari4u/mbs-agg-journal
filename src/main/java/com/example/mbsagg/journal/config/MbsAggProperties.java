package com.example.mbsagg.journal.config;

import java.time.Duration;
import java.time.LocalTime;
import java.time.ZoneId;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.validation.annotation.Validated;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

/**
 * All application settings under {@code mbsagg.*}. {@code application.yml} maps the original environment variables
 * ({@code JOURNAL_TOPIC}, {@code FLUSH_EVERY}, ...) onto these; any of them can also be set the Spring Boot way, for
 * example {@code MBSAGG_TOPICS_JOURNAL} or {@code --mbsagg.topics.journal=...}.
 *
 * @param zone     time zone of the business day
 * @param rollover time of day from which records count toward the next date ({@code 00:00} = calendar day)
 */
@Validated
@ConfigurationProperties("mbsagg")
public record MbsAggProperties(
        @Valid @NotNull @DefaultValue Topics topics,
        @NotNull @DefaultValue("America/New_York") ZoneId zone,
        @NotNull @DefaultValue("00:00") @DateTimeFormat(pattern = "HH:mm") LocalTime rollover,
        @Valid @NotNull @DefaultValue Aggregator aggregator,
        @Valid @NotNull @DefaultValue Sequencer sequencer,
        @DefaultValue("true") boolean exitOnListenerFailure) {

    /** The four CUSIP-keyed feeds, the journal and the container output. */
    public record Topics(
            @NotBlank @DefaultValue("mbs.positions") String position,
            @NotBlank @DefaultValue("mbs.prices") String price,
            @NotBlank @DefaultValue("mbs.risk") String risk,
            @NotBlank @DefaultValue("mbs.assumptions") String assumption,
            @NotBlank @DefaultValue("mbs.journal") String journal,
            @NotBlank @DefaultValue("mbs.containers") String output) {
    }

    /**
     * @param instanceId       static group membership ({@code group.instance.id}); one per pod
     * @param flushEvery       flush after this many journal records
     * @param flushInterval    or after this long, if anything is pending
     * @param pollTimeout      consumer poll timeout; keep it well below {@code flushInterval}
     * @param startOfDayMargin how much earlier than the rollover a start-of-day replay begins
     */
    public record Aggregator(
            @NotBlank @DefaultValue("mbs-aggregator") String groupId,
            @NotBlank @DefaultValue("local") String instanceId,
            @Positive @DefaultValue("500") int flushEvery,
            @NotNull @DefaultValue("100ms") Duration flushInterval,
            @NotNull @DefaultValue("20ms") Duration pollTimeout,
            @NotNull @DefaultValue("10m") Duration startOfDayMargin) {
    }

    /** @param deadLetterTopic where unparseable feed records go; if unset they are only logged */
    public record Sequencer(
            @NotBlank @DefaultValue("mbs-sequencer") String groupId,
            String deadLetterTopic) {
    }
}
