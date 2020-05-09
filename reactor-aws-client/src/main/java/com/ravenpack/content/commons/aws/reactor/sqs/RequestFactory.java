package com.ravenpack.content.commons.aws.reactor.sqs;

import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Flux;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

@RequiredArgsConstructor
class RequestFactory
{

    private final int maximumBatchSize;

    private final Duration maximumBatchWait;


    <T> SendMessageBatchRequestEntry createSendMessageBatchRequestEntry(
        Function<T, String> toString,
        Map.Entry<Long, T> event)
    {
        return SendMessageBatchRequestEntry
            .builder()
            .id(String.valueOf(event.getKey()))
            .messageBody(toString.apply(event.getValue()))
            .build();
    }

    <T> Flux<SendMessageBatchRequest> createSendMessageBatchRequest(
        Map<Long, T> indexedMessages,
        String queueUrl,
        Function<T, String> toString)
    {
        return Flux.fromIterable(indexedMessages.entrySet())
            .map(event -> createSendMessageBatchRequestEntry(toString, event)
            )
            .collectList()
            .map(entries -> createSendMessageBatchRequest(queueUrl, entries)
            ).flux();
    }

    SendMessageBatchRequest createSendMessageBatchRequest(
        String queueUrl,
        List<SendMessageBatchRequestEntry> entries)
    {
        return SendMessageBatchRequest
            .builder()
            .queueUrl(queueUrl)
            .entries(entries)
            .build();
    }

    ReceiveMessageRequest createReceiveMessageRequest(String queueUrl) {
        return  ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .maxNumberOfMessages(maximumBatchSize)
                .waitTimeSeconds(Math.toIntExact(maximumBatchWait.getSeconds()))
                .build();
    }

    Flux<DeleteMessageBatchRequest> createDeleteMessageBatchRequest(
        Map<Long, Message> indexedMessages,
        String queueUrl)
    {
        return Flux.fromIterable(indexedMessages.entrySet())
            .map(this::createDeleteMessageBatchRequestEntry)
            .collectList()
            .map(entries -> createDeleteMessageBatchRequest(queueUrl, entries))
            .flux();
    }

    DeleteMessageBatchRequestEntry createDeleteMessageBatchRequestEntry(Map.Entry<Long, Message> e)
    {
        return DeleteMessageBatchRequestEntry.builder()
            .id(String.valueOf(e.getKey()))
            .receiptHandle(e.getValue().receiptHandle())
            .build();
    }

    DeleteMessageBatchRequest createDeleteMessageBatchRequest(
        String queueUrl,
        List<DeleteMessageBatchRequestEntry> e)
    {
        return DeleteMessageBatchRequest.builder()
            .queueUrl(queueUrl)
            .entries(e)
            .build();
    }
}
