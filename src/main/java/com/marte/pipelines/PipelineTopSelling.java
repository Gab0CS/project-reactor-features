package com.marte.pipelines;

import com.marte.database.Database;
import com.marte.models.Videogame;
import reactor.core.publisher.Flux;

public class PipelineTopSelling {


    //Return all names of videogames with sales > 80
    public static Flux<String> getTopSalesVideogames(){
        return Database.getVideogamesFlux()
                .filter(videogame -> videogame.getTotalSold() > 80)
                .map(Videogame::getName);
    }
}
