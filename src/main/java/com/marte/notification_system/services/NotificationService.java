package com.marte.notification_system.services;

import com.marte.notification_system.models.NotificationEvent;
import reactor.core.publisher.Mono;

public interface NotificationService {
    Mono<Boolean> sendNotification(NotificationEvent event);
}
