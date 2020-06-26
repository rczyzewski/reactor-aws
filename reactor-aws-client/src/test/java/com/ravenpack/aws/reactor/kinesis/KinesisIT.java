package com.ravenpack.aws.reactor.kinesis;

import com.ravenpack.aws.reactor.Localstack;
import com.ravenpack.aws.reactor.TestHelperCloudWatch;
import com.ravenpack.aws.reactor.TestHelperDynamoDB;
import com.ravenpack.aws.reactor.TestHelperKinesis;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordResponse;
import software.amazon.awssdk.services.kinesis.model.StreamStatus;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.retrieval.polling.PollingConfig;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.Executors;

import static software.amazon.kinesis.common.InitialPositionInStream.TRIM_HORIZON;
import static software.amazon.kinesis.common.InitialPositionInStreamExtended.newInitialPosition;

@Testcontainers
@Slf4j
class KinesisIT
{

    private static final String STREAM = "aKinesisStream";

    @Container
    private static final Localstack localstack =  new Localstack()
            .withServices(Localstack.Service.KINESIS)
            .withLogConsumer(new Slf4jLogConsumer(log));


    TestHelperCloudWatch testHelperCloudWatch = new TestHelperCloudWatch(localstack);
    TestHelperKinesis  testHelperKinesis = new TestHelperKinesis(localstack);
    TestHelperDynamoDB testHelperDynamoDB  = new TestHelperDynamoDB(localstack);



    @BeforeAll
    static void beforeClass()
    {
        Hooks.onOperatorDebug();
    }

    @Test
    void simpleIntegrationTest()
    {

        testHelperKinesis.createStream(STREAM);

        KinesisAsyncClient kinesisAsyncClient = testHelperKinesis.getKinesisAsyncClient();

        /* Waiting untill STREAM is active */
        Flux.range(0, 100)
            .delayElements(Duration.ofMillis(10))
            .log()
            .flatMap(ignored -> Mono.fromFuture(kinesisAsyncClient
                                                    .describeStream(DescribeStreamRequest.builder().streamName(STREAM)
                                                                        .build()))
                .filter(it -> it.streamDescription().streamStatus().equals(StreamStatus.ACTIVE))).blockFirst();

        int numberOfRecords = 50;
        Flux.range(0, numberOfRecords)
            .map(it -> PutRecordRequest.builder()
                .partitionKey("sth" + it)
                .streamName(STREAM)
                .data(SdkBytes.fromString("test" + it, StandardCharsets.UTF_8))
                .build())
            .map(kinesisAsyncClient::putRecord)
            .flatMap(Mono::fromFuture)
            .map(PutRecordResponse::sequenceNumber)
            .log("PUT")
            .blockLast();

        SchedulerFactory schedulerFactory = SchedulerFactory
            .builder()
            .streamName(STREAM)
            .appName("thisAppName")
            .workIdentifier("externallyProvidedIdentifier")
            .kinesisAsyncClient(kinesisAsyncClient)
            .dynamoDbAsyncClient(testHelperDynamoDB.getDdbAsyncClient())
            .cloudWatchAsyncClient(testHelperCloudWatch.getCloudWatchAsyncClient())
            .retrievalConfig(configsBuilder -> configsBuilder.retrievalConfig()
                .initialPositionInStreamExtended(newInitialPosition(TRIM_HORIZON))
                .retrievalSpecificConfig(
                    new PollingConfig(
                        configsBuilder
                            .streamName(),
                        configsBuilder.kinesisClient())
                )
            )
            .build();

        RxKinesis rxKinesis = RxKinesis.builder()
            .executorService(() -> Executors.newFixedThreadPool(8))
            .schedulerFactory(schedulerFactory)
            .build();

        StepVerifier.create(rxKinesis.kcl(new WorldConnector<>(numberOfRecords * 2, Duration.ofSeconds(1)))
                                .flatMapIterable(ProcessRecordsInput::records)
                                .timeout(Duration.ofSeconds(20)))
            .expectNextCount(numberOfRecords).verifyError();
    }
}
