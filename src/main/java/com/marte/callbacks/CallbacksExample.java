package com.marte.callbacks;

import com.marte.database.Database;
import com.marte.models.Videogame;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;

import java.security.cert.CertificateRevokedException;
import java.time.Duration;
import java.util.zip.ZipError;

@Slf4j
public class CallbacksExample {

    public static Flux<Videogame> callbacks(){

        return Database.getDataAsFlux()
                .doOnSubscribe(subs -> log.info("[doOnSubscribe]"))
                .doOnRequest((n -> log.info("[doOnRequest]:{}" ,n)))
                .doOnNext(videogame -> log.info("[doOnNext]: {}" ,videogame.getName()))
                .doOnCancel(() -> log.warn("[doOnCancel]:"))
                .doOnError(err -> log.error("[doOnError]: {}" ,err.getMessage()))
                .doOnComplete(() -> log.info("[doOnComplete] success"))
                .doOnTerminate(() -> log.info("[doOnTerminate] terminated"))
                .doFinally(signalType -> log.warn("[doFinally]: {}" ,signalType));
    }
}
