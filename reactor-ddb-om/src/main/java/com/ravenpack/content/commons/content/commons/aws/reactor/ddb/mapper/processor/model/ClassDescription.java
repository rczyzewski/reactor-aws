package com.ravenpack.content.commons.content.commons.aws.reactor.ddb.mapper.processor.model;

import lombok.Builder;
import lombok.Getter;
import lombok.Value;

import java.util.List;

@Value
@Getter
@Builder
public class ClassDescription
{
    private final String name;
    private final String packageName;
    private final List<FieldDescription> fieldDescriptions;
}
