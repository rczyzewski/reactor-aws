package com.ravenpack.content.commons.aws.reactor.kinesis;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.SynchronousSink;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class WorldConnector<T>
{

    private static final Integer POLLING_WAIT = 200;
    private final LinkedBlockingQueue<T> blockingQueue;
    private final AtomicBoolean broken = new AtomicBoolean();
    private final Duration duration;

    WorldConnector(int queueSize, Duration duration)
    {
        this.blockingQueue = new LinkedBlockingQueue<>(queueSize);
        this.duration = duration;

        Flux.interval(Duration.ofSeconds(1)).startWith(0L)
            .doOnNext(it -> log.info("queue size: {} of {}", blockingQueue.size(), queueSize))
            .subscribeOn(Schedulers.newSingle("some-thread"))
            .subscribe();
    }

    Flux<T> take()
    {

        return Flux.generate((SynchronousSink<T> sink) -> {

            log.info("dooing pool on the queue");
            T element = blockingQueue.poll();

            if (broken.get()) {
                log.info("connector broken");

                sink.error(new UnableToProcessMore());
            } else if (element == null) {
                sink.complete();
                log.info("sink compleate");

            } else {
                sink.next(element);
                log.info("sink {}", element);
            }
        })

            .repeatWhen(it -> it.delayElements(Duration.ofMillis(POLLING_WAIT)));
    }

    @SneakyThrows
    public void put(T element)
    {

        log.info("putting something to the queue: {}", element);
        try {
            if (!broken.get() && blockingQueue.offer(element, duration.toMillis(), TimeUnit.MILLISECONDS)) {
                log.info("element in the queue {}", element);
            } else {
                broken.set(true);
                throw new UnableToProcessMore();
            }

        } catch (InterruptedException e) {
            broken.set(true);
            Thread.currentThread().interrupt();
        }

    }

    public static class UnableToProcessMore extends RuntimeException
    {
    }

}
