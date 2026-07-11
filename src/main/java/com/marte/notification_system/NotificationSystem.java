package com.marte.notification_system;

import com.marte.notification_system.models.NotificationEvent;
import com.marte.notification_system.models.NotificationStatus;
import com.marte.notification_system.models.Priority;
import com.marte.notification_system.services.EmailService;
import com.marte.notification_system.services.NotificationService;
import com.marte.notification_system.services.PhoneService;
import com.marte.notification_system.services.TeamsService;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Slf4j
public class NotificationSystem {

    private final Sinks.Many<NotificationEvent> mainEventSink;
    @Getter
    private final Sinks.Many<NotificationEvent> historySink;

    private final NotificationService teamsService;
    private final NotificationService emailService;
    private final NotificationService phoneService;

    private final Sinks.One<NotificationEvent> teamsSink;
    private final Sinks.One<NotificationEvent> emailSink;
    private final Sinks.One<NotificationEvent> phoneSink;

    private final ConcurrentMap<String, NotificationEvent> notificationCache;

    public NotificationSystem(){
        this.mainEventSink = Sinks.many().multicast().onBackpressureBuffer();
        this.historySink = Sinks.many().replay().limit(50);
        this.teamsSink = Sinks.one();
        this.emailSink = Sinks.one();
        this.phoneSink = Sinks.one();

        this.teamsService = new TeamsService();
        this.emailService = new EmailService();
        this.phoneService = new PhoneService();

        this.notificationCache = new ConcurrentHashMap<>();
        this.setUpProcessingFlows();

    }

    private void UpdateEventStatus(NotificationEvent event){
        if(Objects.isNull(event.getStatus())){
            event.setId(UUID.randomUUID().toString());
            event.setStatus(NotificationStatus.PENDING);
            this.notificationCache.put(event.getId(), event);
        }
    }
    private void updateSuccessfullEvent(NotificationEvent event, String chanel){
        log.info("Success event  by: {}, event: {}",chanel, event.getId());
        NotificationEvent cacheEvent = this.notificationCache.get(event.getId());

        if(Objects.nonNull(cacheEvent)){
            cacheEvent.setStatus(NotificationStatus.DELIVERED);
            this.historySink.tryEmitNext(cacheEvent);
        }
    }
    private void updateErrorStatus(NotificationEvent event, String chanel, Throwable error){
        log.error("Error to send notification by: {} for [event]: {}, [error]: {}", chanel, event.getId(), error);
        NotificationEvent cacheEvent = this.notificationCache.get(event.getId());
        if(Objects.nonNull(cacheEvent)){
            cacheEvent.setStatus(NotificationStatus.FAILED);
            this.historySink.tryEmitNext(cacheEvent);
        }
    }

    private void setUpProcessingFlows(){
        this.mainEventSink
                .asFlux()
                .doOnNext(event -> log.info("Received new event: {}" ,event))
                .doOnNext(this::UpdateEventStatus)
                .doOnNext(event -> this.historySink.tryEmitNext(event))
                .subscribe(this::routeEventByPriority);
        this.setupTeamsProcessor();
        this.setupPhoneProcessor();
    }

    private void setupTeamsProcessor() {
        this.teamsSink
                .asMono()
                .repeat()
                .flatMap(event ->
                        this.teamsService.sendNotification(event)
                                .subscribeOn(Schedulers.boundedElastic())
                                .doOnSuccess(success -> this.updateSuccessfullEvent(event, TEAMS_CHANEL))
                                .doOnError(error -> this.updateErrorStatus(event, TEAMS_CHANEL, error))
                                .onErrorResume(error -> Mono.just(false))
                ).subscribe();
    }

    private void setupEmailProcessor() {
        this.teamsSink
                .asMono()
                .repeat()
                .flatMap(event ->
                        this.emailService.sendNotification(event)
                                .subscribeOn(Schedulers.boundedElastic())
                                .doOnSuccess(success -> this.updateSuccessfullEvent(event, EMAIL_CHANEL))
                                .doOnError(error -> this.updateErrorStatus(event, EMAIL_CHANEL, error))
                                .onErrorResume(error -> Mono.just(false))
                ).subscribe();
    }

    private void setupPhoneProcessor() {
        this.teamsSink
                .asMono()
                .repeat()
                .flatMap(event ->
                        this.phoneService.sendNotification(event)
                                .subscribeOn(Schedulers.boundedElastic())
                                .doOnSuccess(success -> this.updateSuccessfullEvent(event, PHONE_CHANEL))
                                .doOnError(error -> this.updateErrorStatus(event, PHONE_CHANEL, error))
                                .retry(3)
                                .onErrorResume(error -> Mono.just(false))
                ).subscribe();
    }

    private void routeEventByPriority(NotificationEvent event){
        this.teamsSink.tryEmitValue(event);
        if (event.getPriority() == Priority.HIHGH || event.getPriority() == Priority.MEDIUM){
            this.emailSink.tryEmitValue(event);
        }
        if (event.getPriority() == Priority.HIHGH){
            this.phoneSink.tryEmitValue(event);
        }
    }

    public void publishEvent(NotificationEvent event){
        this.mainEventSink.tryEmitNext(event);
    }

    public Flux<NotificationEvent> getNotificationHistory(){
        return this.historySink.asFlux();
    }

    public Mono<NotificationEvent> getNotificationHistoryById(String id){
        return Mono.justOrEmpty(this.notificationCache.get(id));
    }

    public Flux<NotificationEvent> tryFailedNotification(){
        return Flux.fromIterable(this.notificationCache.values())
                .filter(event -> event.getStatus() == NotificationStatus.FAILED)
                .doOnNext(this::publishEvent);
    }

    private static final String TEAMS_CHANEL = "Teams";
    private static final String EMAIL_CHANEL = "Email";
    private static final String PHONE_CHANEL = "Phone";
}
