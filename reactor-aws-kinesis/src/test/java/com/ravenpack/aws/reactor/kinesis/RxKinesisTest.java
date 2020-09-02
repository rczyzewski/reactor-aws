package com.ravenpack.aws.reactor.kinesis;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.test.StepVerifier;
import software.amazon.kinesis.coordinator.Scheduler;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Slf4j
@ExtendWith(MockitoExtension.class)
class RxKinesisTest
{

    static {
        Hooks.onOperatorDebug();
    }

    @Mock
    private Supplier<ExecutorService> serviceSupplier;
    @Mock
    private ExecutorService es;
    @Mock
    private SchedulerFactory sf;
    @Mock
    private Scheduler scheduler;
    @Mock
    private WorldConnector<ProcessRecordsInput> worldConnector;
    private RxKinesis rxKinesis;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void prepare()
    {
        ArgumentCaptor<Consumer<ProcessRecordsInput>> cons = ArgumentCaptor.forClass(Consumer.class);
        when(sf.createScheduler(cons.capture())).thenReturn(scheduler);
        when(serviceSupplier.get()).thenReturn(es);
        rxKinesis = RxKinesis.builder().executorService(serviceSupplier).schedulerFactory(sf).build();
    }

    @Test
    void testWithoutData()
    {

        RxKinesis rxKinesis = RxKinesis.builder().executorService(serviceSupplier).schedulerFactory(sf).build();

        when(worldConnector.take()).thenReturn(Flux.empty());

        Flux<ProcessRecordsInput> f = rxKinesis.kcl(worldConnector);
        StepVerifier.create(
            f.window(Duration.ofSeconds(1)).take(1).flatMap(it -> it))
            .verifyComplete();

        verify(scheduler, times(1)).shutdown();
    }

    @Test
    void pretendingThereAreData()
    {

        ProcessRecordsInput processRecordsInput = ProcessRecordsInput.builder().build();

        when(worldConnector.take()).thenReturn(Flux.just(processRecordsInput));

        Flux<ProcessRecordsInput> f = rxKinesis.kcl(worldConnector);

        StepVerifier.create(f
                                .window(Duration.ofSeconds(1)).take(1).flatMap(it -> it))
            .expectNext(processRecordsInput)
            .verifyComplete();

        verify(scheduler, times(1)).shutdown();
        verify(worldConnector, times(1)).take();
    }

    @Test
    void nastyExceptionSimulation()
    {

        when(worldConnector.take()).thenReturn(Flux.error(WorldConnector.UnableToProcessMore::new));

        StepVerifier.create(rxKinesis.kcl(worldConnector))
            .expectError(WorldConnector.UnableToProcessMore.class)
            .verify();

        verify(scheduler, times(1)).shutdown();
    }
}