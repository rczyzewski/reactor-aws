/*
 * Copyright (c) 2019 Ravenpack
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ravenpack.aws.reactor.kinesis;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.kinesis.lifecycle.events.InitializationInput;
import software.amazon.kinesis.lifecycle.events.LeaseLostInput;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.lifecycle.events.ShardEndedInput;
import software.amazon.kinesis.lifecycle.events.ShutdownRequestedInput;
import software.amazon.kinesis.processor.ShardRecordProcessor;

import java.util.function.Consumer;

@Slf4j
@AllArgsConstructor
public class RecordProcessor implements ShardRecordProcessor
{

    private Consumer<ProcessRecordsInput> recordConsumer;

    @Override
    public void initialize(InitializationInput initializationInput)
    {
        log.info("Initializing RecordProcessor");
    }

    @Override
    public void processRecords(ProcessRecordsInput processRecordsInput)
    {
        log.info("RecordProcessor is executed");
        recordConsumer.accept(processRecordsInput);
    }

    @Override
    public void leaseLost(LeaseLostInput leaseLostInput)
    {
        log.info("Lost lease");
    }

    @Override
    public void shardEnded(ShardEndedInput shardEndedInput)
    {
        log.info("Shard ended");

    }

    @Override
    public void shutdownRequested(ShutdownRequestedInput shutdownRequestedInput)
    {
        log.info("Shutting down");
    }
}
