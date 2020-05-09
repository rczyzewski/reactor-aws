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
package com.ravenpack.content.commons.aws.reactor.ddb;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;

public interface BaseRepository<T>
{
    Mono<T> create(T item);

    Mono<Void> delete(T item);

    Flux<T> getAll();

    Mono<T> update(T data);

    CreateTableRequest createTable();
}
