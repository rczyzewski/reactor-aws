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
package com.ravenpack.content.commons.aws.reactor.kinesis;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import software.amazon.kinesis.coordinator.Scheduler;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;

import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

@Slf4j
@Builder
@AllArgsConstructor
public class RxKinesis
{

    private static final long MAX_WAITING_TIME = 60L;
    private SchedulerFactory schedulerFactory;

    private Supplier<ExecutorService> executorService;

    @SneakyThrows
    private static void stopService(ExecutorService executorService, Scheduler scheduler)
    {
        log.info("Stopping KCL.");
        scheduler.shutdown();
        log.info("KCL stopped.");
    }

    public Flux<ProcessRecordsInput> kcl(WorldConnector<ProcessRecordsInput> worldConnector)
    {

        Scheduler scheduler = schedulerFactory.createScheduler(worldConnector::put);

        ExecutorService executor = executorService.get();
        executor.execute(scheduler);

        return worldConnector.take()
            .doOnTerminate(() -> RxKinesis.stopService(executor, scheduler))
            .subscribeOn(Schedulers.elastic());
    }

}
