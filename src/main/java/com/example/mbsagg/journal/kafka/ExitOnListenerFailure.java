package com.example.mbsagg.journal.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBooleanProperty;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.event.ConsumerStoppedEvent;
import org.springframework.stereotype.Component;

/**
 * Both roles stop their listener container on any unexpected error (fencing, a failed send, Redis down), because the
 * in-memory state can't be trusted afterwards. This closes the application with a non-zero exit code when that
 * happens, so Kubernetes restarts the pod and the new instance resumes from the committed offsets.
 * {@code mbsagg.exit-on-listener-failure=false} turns it off (the tests do).
 */
@Component
@ConditionalOnBooleanProperty(name = "mbsagg.exit-on-listener-failure", matchIfMissing = true)
class ExitOnListenerFailure {
    private static final Logger LOG = LoggerFactory.getLogger(ExitOnListenerFailure.class);

    private final ConfigurableApplicationContext context;

    ExitOnListenerFailure(ConfigurableApplicationContext context) {
        this.context = context;
    }

    @EventListener
    void onConsumerStopped(ConsumerStoppedEvent event) {
        if (event.getReason() == ConsumerStoppedEvent.Reason.NORMAL) return;
        LOG.error("listener container stopped ({}): exiting so the pod restarts", event.getReason());
        // Not on the consumer thread: closing the context waits for that thread to finish.
        Thread exit = new Thread(() -> System.exit(SpringApplication.exit(context, () -> 1)), "mbsagg-exit");
        exit.start();
    }
}
