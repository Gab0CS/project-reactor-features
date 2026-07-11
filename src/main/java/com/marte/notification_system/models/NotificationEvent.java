package com.marte.notification_system.models;

import lombok.Data;
import org.apache.logging.log4j.core.appender.rolling.SizeBasedTriggeringPolicy;

import java.time.LocalDateTime;

@Data
public class NotificationEvent {
    private String id;
    private String source;
    private String message;
    private Priority priority;
    private LocalDateTime timeStamp;
    private NotificationStatus status;
}
