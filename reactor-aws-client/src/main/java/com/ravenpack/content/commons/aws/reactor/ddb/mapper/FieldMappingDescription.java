package com.ravenpack.content.commons.aws.reactor.ddb.mapper;

import lombok.Value;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

@Value
public class FieldMappingDescription<T>
{
    String ddbName;
    boolean keyValue;
    BiFunction<T, AttributeValue, T> wither;
    Function<T, Optional<AttributeValue>> export;
}
