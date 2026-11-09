package com.example.mbsagg.journal.support;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

/** Counts verdicts over all seeds of a scenario and prints one table row. */
public final class Tally {
    private final String name;
    private final List<Verdict> runs = new ArrayList<>();

    public Tally(String name) {
        this.name = name;
    }

    public void add(Verdict v) {
        runs.add(v);
    }

    public int runs() {
        return runs.size();
    }

    private int count(Predicate<Verdict> p) {
        return (int) runs.stream().filter(p).count();
    }

    public int downstream() {
        return count(Verdict::downstream);
    }

    public int state() {
        return count(Verdict::state);
    }

    public int monotonic() {
        return count(Verdict::monotonic);
    }

    public int snapshot() {
        return count(Verdict::snapshot);
    }

    public int fenced() {
        return count(Verdict::fencedOk);
    }

    public boolean allCorrect() {
        int n = runs();
        return downstream() == n && state() == n && monotonic() == n && snapshot() == n && fenced() == n;
    }

    public String row() {
        int n = runs();
        return String.format("%-56s output %2d/%d  state %2d/%d  never-back %2d/%d  redis %2d/%d  fenced %2d/%d",
                name, downstream(), n, state(), n, monotonic(), n, snapshot(), n, fenced(), n);
    }
}
