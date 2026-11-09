package com.example.mbsagg.journal.support;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;

import org.junit.jupiter.api.Assumptions;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;

/**
 * A Redis 7 for the tests, shared by the whole test run. Uses {@code MBSAGG_TEST_REDIS=host:port} if set (the tests
 * FLUSHALL it, so point it at a disposable instance), otherwise starts {@code redis-server} from the PATH on a free
 * port. {@link #get()} skips the calling test if neither is available.
 */
public final class TestRedis {
    private static TestRedis instance;
    private static String unavailable;

    public final String host;
    public final int port;
    public final StringRedisTemplate template;
    private final LettuceConnectionFactory connections;

    private TestRedis(String host, int port) {
        this.host = host;
        this.port = port;
        this.connections = new LettuceConnectionFactory(new RedisStandaloneConfiguration(host, port));
        connections.afterPropertiesSet();
        connections.start();
        this.template = new StringRedisTemplate(connections);
    }

    /** The shared instance; skips the test (JUnit assumption) if there is no Redis. */
    public static synchronized TestRedis get() {
        if (instance == null && unavailable == null) start();
        Assumptions.assumeTrue(instance != null, unavailable);
        return instance;
    }

    /** For {@code @EnabledIf}: true if a Redis is (or can be) running. */
    public static synchronized boolean available() {
        if (instance == null && unavailable == null) start();
        return instance != null;
    }

    private static void start() {
        String external = System.getenv("MBSAGG_TEST_REDIS");
        if (external != null && !external.isEmpty()) {
            String[] hp = external.split(":");
            instance = new TestRedis(hp[0], Integer.parseInt(hp[1]));
            return;
        }
        try {
            int port;
            try (ServerSocket s = new ServerSocket(0)) {
                port = s.getLocalPort();
            }
            File dir = Files.createTempDirectory("mbsagg-redis").toFile();
            Process p = new ProcessBuilder("redis-server", "--port", Integer.toString(port), "--save", "",
                    "--appendonly", "no", "--dir", dir.getAbsolutePath())
                    .redirectOutput(ProcessBuilder.Redirect.DISCARD).redirectErrorStream(true).start();
            Runtime.getRuntime().addShutdownHook(new Thread(p::destroy));
            TestRedis r = new TestRedis("127.0.0.1", port);
            for (int i = 0; i < 100; i++) {
                try {
                    r.ping();
                    instance = r;
                    return;
                } catch (RuntimeException e) {
                    Thread.sleep(50);
                }
            }
            unavailable = "redis-server did not start";
        } catch (IOException e) {
            unavailable = "no redis-server on PATH and MBSAGG_TEST_REDIS not set: " + e.getMessage();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            unavailable = "interrupted";
        }
    }

    private void ping() {
        template.execute(connection -> connection.ping(), true);
    }

    /** Empties Redis. */
    public void reset() {
        template.execute(connection -> {
            connection.serverCommands().flushAll();
            return null;
        }, true);
    }
}
