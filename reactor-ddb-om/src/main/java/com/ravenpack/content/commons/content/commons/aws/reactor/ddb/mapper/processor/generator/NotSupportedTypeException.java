package com.ravenpack.content.commons.content.commons.aws.reactor.ddb.mapper.processor.generator;

import com.ravenpack.content.commons.content.commons.aws.reactor.ddb.mapper.processor.model.FieldDescription;

public class NotSupportedTypeException extends RuntimeException
{
    NotSupportedTypeException(FieldDescription fieldDescription)
    {
        super(fieldDescription.toString());
    }
}
