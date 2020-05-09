package com.ravenpack.content.commons.content.commons.aws.reactor.ddb.mapper.processor.model;

import lombok.Builder;
import lombok.Value;

import java.util.List;

@Builder
@Value
public class IndexDescription
{
    private final String name;
    private final FieldDescription hashField;
    private final FieldDescription rangeField;
    private final List<FieldDescription> attributes;
}
