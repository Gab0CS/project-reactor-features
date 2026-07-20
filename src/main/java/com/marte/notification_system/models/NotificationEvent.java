package com.marte.notification_system.models;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.logging.log4j.core.appender.rolling.SizeBasedTriggeringPolicy;

import java.time.LocalDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class NotificationEvent {
    private String id;
    private String source;
    private String message;
    private Priority priority;
    private LocalDateTime timeStamp;
    private NotificationStatus status;
}
