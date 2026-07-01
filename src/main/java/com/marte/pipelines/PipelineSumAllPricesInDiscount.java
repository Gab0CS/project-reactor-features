package com.marte.pipelines;

import com.marte.database.Database;
import com.marte.models.Videogame;
import reactor.core.publisher.Mono;

public class PipelineSumAllPricesInDiscount {

    public static Mono<Double> getSumAllPricesInDiscount(){
        return Database.getVideogamesFlux()
                .filter(videogame -> !videogame.getIsDiscount())
                .map(Videogame::getPrice)
                .reduce(0.0, Double::sum);
    }
}
