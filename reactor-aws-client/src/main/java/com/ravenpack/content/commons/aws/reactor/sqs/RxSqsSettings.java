package com.ravenpack.content.commons.aws.reactor.sqs;

import lombok.Builder;
import lombok.Value;

import java.time.Duration;

@Value
@Builder
public class RxSqsSettings
{
    @Builder.Default
    int maximumBatchSize = 10;

    @Builder.Default
    Duration maximumBatchWait = Duration.ofSeconds(20);

    @Builder.Default
    int parallelism = 4;

    @Builder.Default
    boolean closeOnEmptyReceive = false;

    public static RxSqsSettings create()
    {
        return RxSqsSettings.builder().build();
    }
}
