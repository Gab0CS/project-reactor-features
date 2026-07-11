package com.marte.error_handler;

import com.marte.database.Database;
import com.marte.models.Console;
import com.marte.models.Videogame;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;

@Slf4j
public class FallbackService {

    public static Flux<Videogame> callFallback() {

        return Database.getDataAsFlux()
                .handle((vg, sink) -> {

                    if (Console.DISABLED == vg.getConsole()) {
                        sink.error(new RuntimeException("Videogame is disabled"));
                        return;
                    }

                    sink.next(vg);
                })
                .retry(5)
                .onErrorResume(error -> {
                    log.error("Database is failing");

                    return Database.fluxFallback;
                })
                .repeat(1)
                .cast(Videogame.class);
    }
}
