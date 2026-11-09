package com.example.mbsagg.journal.support;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.example.mbsagg.journal.FeedSource;
import com.example.mbsagg.journal.JournalRecord;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

/**
 * A synthetic business day for one journal partition: four feeds (a record's version is its offset in its feed),
 * merged by the sequencer in a random order, with occasional re-sends of the last few records (at-least-once).
 */
public final class SimJournal {
    public static final String DATE = "2026-11-09";
    private static final JsonMapper MAPPER = JsonMapper.shared();
    private static final FeedSource[] FEEDS = {FeedSource.POSITION, FeedSource.RISK, FeedSource.PRICE, FeedSource.ASSUMPTION};
    private static final int[] WEIGHTS = {3, 3, 5, 1};

    private SimJournal() {
    }

    public static Map<FeedSource, List<JournalRecord>> feeds(long seed) {
        return feeds(seed, 40, 2, 3, 6000);
    }

    public static Map<FeedSource, List<JournalRecord>> feeds(long seed, int nCusips, int nProducts, int nInventories,
                                                             int nEvents) {
        Random rnd = new Random(seed);
        List<String> cusips = new ArrayList<>();
        for (int n = 0; n < nCusips; n++) cusips.add(String.format("3140X%04d", n));
        List<String[]> containers = new ArrayList<>();
        for (String c : cusips) {
            for (int p = 0; p < nProducts; p++) {
                for (int i = 0; i < nInventories; i++) {
                    if (rnd.nextDouble() < 0.6) containers.add(new String[]{"P" + p, "INV" + i, c});
                }
            }
        }
        Map<FeedSource, List<JournalRecord>> feeds = new LinkedHashMap<>();
        for (FeedSource f : FEEDS) feeds.put(f, new ArrayList<>());
        for (int e = 0; e < nEvents; e++) {
            FeedSource src = pick(rnd);
            List<JournalRecord> feed = feeds.get(src);
            ObjectNode v = MAPPER.createObjectNode();
            if (src == FeedSource.POSITION) {
                String[] k = containers.get(rnd.nextInt(containers.size()));
                boolean closed = rnd.nextDouble() < 0.06;
                v.put("face", closed ? 0 : (1 + rnd.nextInt(1000)) * 1000L);
                v.put("closed", closed);
                feed.add(new JournalRecord(src, DATE, k[2], JournalRecord.containerKey(k[0], k[1], k[2]), feed.size(), v));
            } else {
                String c = cusips.get(rnd.nextInt(cusips.size()));
                switch (src) {
                    case PRICE -> v.put("px", round(90 + 15 * rnd.nextDouble(), 4));
                    case RISK -> v.put("oad", round(-2 + 10 * rnd.nextDouble(), 3));
                    default -> v.put("cpr", round(2 + 18 * rnd.nextDouble(), 2));
                }
                feed.add(new JournalRecord(src, DATE, c, null, feed.size(), v));
            }
        }
        return feeds;
    }

    /** The sequencer's output: feeds merged in a random order, with re-sends at {@code dupRate}. */
    public static List<JournalRecord> journal(Map<FeedSource, List<JournalRecord>> feeds, Random rnd, double dupRate) {
        Map<FeedSource, Integer> idx = new LinkedHashMap<>();
        feeds.keySet().forEach(f -> idx.put(f, 0));
        List<JournalRecord> out = new ArrayList<>();
        while (true) {
            List<FeedSource> live = new ArrayList<>();
            for (FeedSource f : feeds.keySet()) if (idx.get(f) < feeds.get(f).size()) live.add(f);
            if (live.isEmpty()) return out;
            FeedSource s = live.get(rnd.nextInt(live.size()));
            out.add(feeds.get(s).get(idx.get(s)));
            idx.put(s, idx.get(s) + 1);
            if (dupRate > 0 && idx.get(s) > 5 && rnd.nextDouble() < dupRate) {
                idx.put(s, idx.get(s) - (1 + rnd.nextInt(5)));   // the sequencer re-sends
            }
        }
    }

    public static List<JournalRecord> day(long seed, double dupRate) {
        return journal(feeds(seed), new Random(seed * 7), dupRate);
    }

    private static FeedSource pick(Random rnd) {
        int x = rnd.nextInt(12);
        for (int i = 0; i < FEEDS.length; i++) {
            if (x < WEIGHTS[i]) return FEEDS[i];
            x -= WEIGHTS[i];
        }
        throw new IllegalStateException();
    }

    private static double round(double v, int places) {
        double f = Math.pow(10, places);
        return Math.round(v * f) / f;
    }
}
