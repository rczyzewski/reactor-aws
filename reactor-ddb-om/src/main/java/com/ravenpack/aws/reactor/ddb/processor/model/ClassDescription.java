package com.ravenpack.aws.reactor.ddb.processor.model;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;
import lombok.Value;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
@Builder
public class ClassDescription
{
    private final String name;
    private final String packageName;
    private final List<FieldDescription> fieldDescriptions;

    @ToString.Exclude
    private final Map<String,ClassDescription> sourandingClasses;


}
