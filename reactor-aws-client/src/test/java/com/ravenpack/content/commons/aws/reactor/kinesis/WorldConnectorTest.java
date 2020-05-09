package com.ravenpack.content.commons.aws.reactor.kinesis;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.function.UnaryOperator;

@Slf4j
class WorldConnectorTest
{

    @Test
    void checkConnectorBecomesBroken()
    {

        WorldConnector<String> a = new WorldConnector<>(1, Duration.ofSeconds(1));

        //We will provoke, WorldConnector to fail with put
        StepVerifier.create(Flux.just("A", "B", "C")
            .flatMap(it -> Mono.fromCallable(() -> {
                a.put(it);
                return it;
            }))
        ).expectNext("A")
            .verifyError(WorldConnector.UnableToProcessMore.class);

        //Once it failed, it can't work correctly anymore
        // - exception is propagated into reactive stream.
        StepVerifier.create(a.take()).verifyError(WorldConnector.UnableToProcessMore.class);
    }

    String putWrapper(UnaryOperator<String> putFunction, String arg)
    {

        long timeMillis = System.currentTimeMillis();
        try {
            String ret = putFunction.apply(arg);
            log.info("total time for putting element {} {} ", arg, System.currentTimeMillis() - timeMillis);
            return ret;
        } catch (WorldConnector.UnableToProcessMore e) {
            log.info("failed to put element {} {}", arg, System.currentTimeMillis() - timeMillis);
            throw e;
        }
    }

    @Test
    @SneakyThrows
    void checkSlowProcessingAdjustProducerSpeed()
    {

        int count = 100;

        WorldConnector<String> a = new WorldConnector<>(5, Duration.ofSeconds(1));

        Flux.range(0, count)
            .map(Object::toString)
            .map(it -> putWrapper(arg -> {
                a.put(arg);
                return arg;
            }, it))
            .subscribeOn(Schedulers.elastic())
            .subscribe();

        StepVerifier.create(
            a.take()
                .flatMap(it -> Mono.just(it).delayElement(Duration.ofMillis(50)), 10)
                .timeout(Duration.ofSeconds(1))
        )
            .expectNextCount(count)
            .expectError(java.util.concurrent.TimeoutException.class)
            .verify();
    }
}