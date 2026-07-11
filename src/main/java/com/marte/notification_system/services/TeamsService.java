package com.marte.notification_system.services;

import com.marte.notification_system.models.NotificationEvent;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;

import java.util.concurrent.ThreadLocalRandom;
@Slf4j
public class TeamsService implements NotificationService{

    public Mono<Boolean> sendNotification(NotificationEvent event){

        return Mono.fromCallable(() -> {

            Thread.sleep(150);
            if(ThreadLocalRandom.current().nextInt(10) == 0){
                throw new RuntimeException("Error on sending Teams notification");
            }
            log.info("Teams message sended successfully: {}" ,event);
            return true;
        });
    }
}
