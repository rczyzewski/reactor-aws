package com.ravenpack.aws.reactor.util;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.util.function.Tuple2;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class RxUtils
{

    public static <T> Function<Flux<List<T>>, Publisher<Map<Long, T>>> toMapWithIndex()
    {
        return f ->
            f.flatMap(
                list ->
                    Flux.fromIterable(list)
                        .index()
                        .collectMap(Tuple2::getT1, Tuple2::getT2));
    }
}
