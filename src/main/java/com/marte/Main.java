package com.marte;

import com.marte.pipelines.PipelineSumAllPricesInDiscount;
import com.marte.pipelines.PipelineTopSelling;
import lombok.extern.java.Log;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Log
public class Main {
    public static void main(String[] args) {
        /*Mono<String> mono = Mono.just("Hello world I'm a mono")
                .doOnNext(value -> log.info("[onNext]: " + value))
                .doOnSuccess(value -> log.info("[onSuccess]: " + value))
                .doOnError(err -> log.info("[error]+ " + err.getMessage()));

        mono.subscribe(
                data -> log.info("Receiving data: " + data),
                err -> log.info("Error: " + err.getMessage()),
                () -> log.info("completed successfully")
        );



        Flux<String> flux = Flux.just("Java", "Spring", "Reactor", "R2DBC")
                .doOnNext(value -> log.info("[onNext]: " + value))
                .doOnComplete(() -> log.info("[doOnComplete]: Success"));
        flux.subscribe(
                data -> log.info("receiving data: " + data),
                err ->log.info("Error: " + err.getMessage()),
                () -> log.info("Completed success")
        );*/

        PipelineTopSelling.getTopSalesVideogames()
                .subscribe(System.out::println);

        PipelineSumAllPricesInDiscount.getSumAllPricesInDiscount().subscribe(System.out::println);
    }
}