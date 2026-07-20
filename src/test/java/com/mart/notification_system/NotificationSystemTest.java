package com.mart.notification_system;

import com.marte.notification_system.NotificationSystem;
import com.marte.notification_system.models.NotificationEvent;
import com.marte.notification_system.models.NotificationStatus;
import com.marte.notification_system.models.Priority;
import com.marte.notification_system.services.EmailService;
import com.marte.notification_system.services.NotificationService;
import com.marte.notification_system.services.PhoneService;
import com.marte.notification_system.services.TeamsService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.test.scheduler.VirtualTimeScheduler;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.any;

public class NotificationSystemTest {
    private NotificationService mockTeamsService;
    private NotificationService mockEmailService;
    private NotificationService mockPhoneService;

    private NotificationSystem notificationSystem;
    private AtomicInteger teamsCallCount;
    private AtomicInteger emailCallCount;
    private AtomicInteger phoneCallCount;

    @BeforeEach
    void setup(){
        this.teamsCallCount = new AtomicInteger(0);
        this.emailCallCount = new AtomicInteger(0);
        this.phoneCallCount = new AtomicInteger(0);

        this.mockTeamsService = mock(TeamsService.class);
        this.mockEmailService = mock(EmailService.class);
        this.mockPhoneService = mock(PhoneService.class);

        when(this.mockTeamsService.sendNotification(any(NotificationEvent.class)))
                .thenAnswer(invocationOnMock -> {
                    this.teamsCallCount.incrementAndGet();
                    return Mono.just(true);
                });
        when(this.mockEmailService.sendNotification(any(NotificationEvent.class)))
                .thenAnswer(invocationOnMock -> {
                    this.emailCallCount.incrementAndGet();
                    return Mono.just(true);
                });
        when(this.mockPhoneService.sendNotification(any(NotificationEvent.class)))
                .thenAnswer(invocationOnMock -> {
                    this.phoneCallCount.incrementAndGet();
                    return Mono.just(true);
                });

        this.notificationSystem = new NotificationSystem(
                mockTeamsService,
                mockEmailService,
                mockPhoneService
        );
    }
    @Test
    @DisplayName("Should send low priority event")
    void testLowPriority(){
        NotificationEvent event =  this.createTestEvent(Priority.LOW);

        this.notificationSystem.publishEvent(event);

        this.sleep(500);

        verify(mockTeamsService, times(1)).sendNotification(any());
        verify(mockEmailService, never()).sendNotification(any());
        verify(mockPhoneService, never()).sendNotification(any());

        assert this.teamsCallCount.get() == 1;
        assert this.emailCallCount.get() == 0;
        assert this.phoneCallCount.get() == 0;
    }
    @Test
    @DisplayName("Should send Medium priority event") void testMediumPriority(){
        NotificationEvent event =  this.createTestEvent(Priority.MEDIUM);

        this.notificationSystem.publishEvent(event);


        verify(mockTeamsService, times(1)).sendNotification(any());
        verify(mockEmailService, times(1)).sendNotification(any());
        verify(mockPhoneService, never()).sendNotification(any());

        assert this.teamsCallCount.get() == 1;
        assert this.emailCallCount.get() == 1;
        assert this.phoneCallCount.get() == 0;
    }
    @Test
    @DisplayName("Should send HIGH priority event") void testHighPriority(){
        NotificationEvent event =  this.createTestEvent(Priority.HIGH);

        this.notificationSystem.publishEvent(event);

        this.sleep(500);

        verify(mockTeamsService, times(1)).sendNotification(any());
        verify(mockEmailService, times(1)).sendNotification(any());
        verify(mockPhoneService, times(1)).sendNotification(any());

        assert this.teamsCallCount.get() == 1;
        assert this.emailCallCount.get() == 1;
        assert this.phoneCallCount.get() == 1;
    }
    @Test
    @DisplayName("Should history keep three elements")
    void shouldHistoryKeepThreeEvents(){
        NotificationEvent event1 =  this.createTestEvent(Priority.LOW);
        NotificationEvent event2 =  this.createTestEvent(Priority.MEDIUM);
        NotificationEvent event3 =  this.createTestEvent(Priority.HIGH);

        this.notificationSystem.publishEvent(event1);
        this.notificationSystem.publishEvent(event2);
        this.notificationSystem.publishEvent(event3);
        StepVerifier.create(notificationSystem.getNotificationHistory().take(3))
                .expectNextCount(3)
                .verifyComplete();
    }
    @Test
    @DisplayName("Should retry three attempts whenp hone service files")
    void TestRetryPhoneAttempts(){
        AtomicInteger attempts = new AtomicInteger(0);
        when(this.mockPhoneService.sendNotification(any(NotificationEvent.class)))
                .thenAnswer(inv -> {
                   int currentAtempt =  attempts.incrementAndGet();
                   if (currentAtempt <= 2){
                       return Mono.error(new RuntimeException("Error on sending phone call notification"));
                   } else {
                       this.phoneCallCount.incrementAndGet();
                       return Mono.just(true);
                   }
                });
        NotificationEvent event = this.createTestEvent(Priority.HIGH);
        this.notificationSystem.publishEvent(event);
        this.sleep(500);
        assert attempts.get() >= 3;
        assert this.phoneCallCount.get() == 1;
    }

    @Test
    @DisplayName("How to use virtual time")
    void testVirtualTime(){
        VirtualTimeScheduler scheduler = VirtualTimeScheduler.create();
        NotificationService teams = mock(NotificationService.class);
        NotificationService email = mock(NotificationService.class);
        NotificationService phone = mock(NotificationService.class);

        when(teams.sendNotification(any(NotificationEvent.class)))
                .thenAnswer(inv -> Mono.just(true).delayElement(Duration.ofMillis(150), scheduler));
        when(email.sendNotification(any(NotificationEvent.class)))
                .thenAnswer(inv -> Mono.just(true).delayElement(Duration.ofMillis(300), scheduler));
        when(phone.sendNotification(any(NotificationEvent.class)))
                .thenAnswer(inv -> Mono.just(true).delayElement(Duration.ofMillis(1000), scheduler));
        NotificationSystem testSystem = new NotificationSystem(
                teams, email, phone);
        NotificationEvent event = this.createTestEvent(Priority.HIGH);
        testSystem.publishEvent(event);
        scheduler.advanceTimeBy(Duration.ofMillis(1500));
        StepVerifier.withVirtualTime(() -> testSystem.getNotificationHistory().take(1))
                .expectNextMatches(element -> element.getStatus() == NotificationStatus.DELIVERED)
                .verifyComplete();
    }

    private NotificationEvent createTestEvent(Priority priority) {
        return NotificationEvent.builder()
                .id(UUID.randomUUID().toString())
                .source("TEST")
                .message("Test msg with priority: " + priority.toString())
                .priority(priority)
                .timeStamp(LocalDateTime.now())
                .status(NotificationStatus.PENDING)
                .build();
    }
    private void sleep(long mills) {
        try {
            Thread.sleep(mills);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
