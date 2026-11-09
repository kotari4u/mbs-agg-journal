package com.example.mbsagg.journal;

import tools.jackson.databind.JsonNode;

/** A value and the version of the message that set it (its offset in its own source partition). */
public record Versioned(JsonNode value, long version) {
}
