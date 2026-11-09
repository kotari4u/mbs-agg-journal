package com.example.mbsagg.journal;

/** The four source feeds. Price, risk and assumptions are CUSIP grain and map one-to-many onto containers. */
public enum FeedSource {
    POSITION(null),
    PRICE("price"),
    RISK("risk"),
    ASSUMPTION("assumptions");

    private final String field;

    FeedSource(String field) {
        this.field = field;
    }

    /** The container field a CUSIP-grain feed sets; {@code null} for positions. */
    public String field() {
        return field;
    }

    public boolean isCusipGrain() {
        return this != POSITION;
    }
}
