package com.example.mbsagg.journal;

/** One output message: a container's current state, keyed by its container key. */
public record ContainerUpdate(String containerKey, String json) {
}
