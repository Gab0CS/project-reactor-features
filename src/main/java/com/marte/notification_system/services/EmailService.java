package com.marte.notification_system.services;

import com.marte.notification_system.models.NotificationEvent;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;

import java.util.concurrent.ThreadLocalRandom;

@Slf4j
public class EmailService implements NotificationService{

    public Mono<Boolean> sendNotification(NotificationEvent event){

        return Mono.fromCallable(() -> {

            Thread.sleep(300);
            if(ThreadLocalRandom.current().nextInt(100) < 15){
                throw new RuntimeException("Error on sending Email notification");
            }
            log.info("Email message sended successfully: {}" ,event);
            return true;
        });
    }
}
