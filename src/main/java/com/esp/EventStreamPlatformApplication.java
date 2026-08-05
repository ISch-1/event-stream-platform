package com.esp;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.modulith.Modulithic;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@Modulithic(systemName = "event-stream-platform")
public class EventStreamPlatformApplication {

    public static void main(String[] args) {
        SpringApplication.run(EventStreamPlatformApplication.class, args);
    }
}
