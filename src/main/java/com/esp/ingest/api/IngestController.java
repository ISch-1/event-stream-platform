package com.esp.ingest.api;

import com.esp.ingest.application.IngestService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1/events")
@RequiredArgsConstructor
@Tag(name = "Ingest", description = "Accept events into the pipeline")
public class IngestController {

    private final IngestService service;

    @PostMapping
    @Operation(summary = "Publish a single event")
    public ResponseEntity<IngestResponse> publish(@Valid @RequestBody IngestRequest body) {
        var out = service.accept(
                body.type(),
                body.userId(),
                body.eventId(),
                body.eventTimeEpochMs(),
                body.payload(),
                body.source());
        return ResponseEntity.status(HttpStatus.ACCEPTED)
                .body(new IngestResponse(out.eventId(), out.topic(), out.partition(), out.offset()));
    }
}
