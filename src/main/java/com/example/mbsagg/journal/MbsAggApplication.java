package com.example.mbsagg.journal;

import java.time.Clock;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.stereotype.Component;

import com.example.mbsagg.journal.config.MbsAggProperties;

/**
 * MBS position aggregation. Run one role per process by activating its Spring profile:
 * <pre>
 *   java -jar mbs-agg-journal.jar --spring.profiles.active=sequencer
 *   java -jar mbs-agg-journal.jar --spring.profiles.active=aggregator
 * </pre>
 * Both profiles can be active in one process (handy locally). Settings: {@link MbsAggProperties} and the standard
 * {@code spring.kafka.*} / {@code spring.data.redis.*} properties.
 */
@EnableKafka
@SpringBootApplication
@ConfigurationPropertiesScan
public class MbsAggApplication {

    public static void main(String[] args) {
        SpringApplication.run(MbsAggApplication.class, args);
    }

    @Bean
    Clock clock(MbsAggProperties properties) {
        return Clock.system(properties.zone());
    }

    /** Fails startup when no role is active, rather than running a process that does nothing. */
    @Component
    @Profile("!sequencer & !aggregator")
    static class NoRoleActive {
        NoRoleActive() {
            throw new IllegalStateException(
                    "No role active: start with --spring.profiles.active=sequencer and/or aggregator");
        }
    }
}
