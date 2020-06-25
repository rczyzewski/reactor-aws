package com.ravenpack.aws.reactor;


import lombok.AllArgsConstructor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.util.Logger;
import reactor.util.Loggers;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.DeleteQueueRequest;

import java.util.logging.Level;

@AllArgsConstructor
public class TestHelperSqs {

    Localstack localstack;

    public String getSqsQueueName(String queueUrl)
    {
        String[] splittedUrl = queueUrl.split("/");
        return splittedUrl[splittedUrl.length - 1];
    }

    public SqsAsyncClient getSqsAsyncClient()
    {
        return SqsAsyncClient.builder()
                .endpointOverride(localstack.getEndpointOverride(Localstack.Service.SQS))
                .credentialsProvider(localstack.getCredentials())
                .build();
    }

    public SqsClient getSqsClient()
    {
        return SqsClient.builder()
                .endpointOverride(localstack.getEndpointOverride(Localstack.Service.SQS))
                .credentialsProvider(localstack.getCredentials())
                .build();
    }


    public String createSqsQueue(String queueName)
    {
        return getSqsClient().createQueue(CreateQueueRequest.builder().queueName(queueName).build())
                .queueUrl();
    }

    public void sqsCleanup()
    {
        Flux.fromIterable(getSqsClient().listQueues().queueUrls())
                .flatMap(queueUrl -> Mono.fromFuture(
                        getSqsAsyncClient().deleteQueue(
                                DeleteQueueRequest.builder()
                                        .queueUrl(queueUrl)
                                        .build())
                        )
                        .log("watch out!", Level.SEVERE, false, SignalType.ON_ERROR)
                        .onErrorResume( e-> Mono.empty())

                )
                .blockLast();
    }

    public void sqsCleanup(String... queueNames)
    {
        Flux.fromArray(queueNames)
                .flatMap(queueUrl -> Mono.fromFuture(
                        getSqsAsyncClient().deleteQueue(
                                DeleteQueueRequest.builder()
                                        .queueUrl(queueUrl)
                                        .build())))
                .onErrorContinue((t, o) -> Mono.empty())
                .blockLast();
    }
}
