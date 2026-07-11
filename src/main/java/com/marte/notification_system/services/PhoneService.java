package com.marte.notification_system.services;

import com.marte.notification_system.models.NotificationEvent;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;

import java.util.concurrent.ThreadLocalRandom;

@Slf4j
public class PhoneService implements NotificationService{

    public Mono<Boolean> sendNotification(NotificationEvent event){

        return Mono.fromCallable(() -> {

            Thread.sleep(1000);
            if(ThreadLocalRandom.current().nextInt(100) < 20){
                throw new RuntimeException("Error on sending phone call notification");
            }
            log.info("Teams message sended successfully: {}" ,event);
            return true;
        });
    }
}
