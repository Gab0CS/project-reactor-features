package com.marte.pipelines;

import com.marte.database.Database;
import com.marte.models.Review;
import reactor.core.publisher.Flux;

public class PipelineAllComments {

    public static Flux<String> getAllReviewComments(){
        return Database.getDataAsFlux()
                .flatMap(videogame -> Flux.fromIterable(videogame.getReviews()))
                .map(review -> review.getComment());
    }
}
