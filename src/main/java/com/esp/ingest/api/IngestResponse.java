package com.esp.ingest.api;

import java.util.UUID;

public record IngestResponse(UUID eventId, String topic, int partition, long offset) {}
