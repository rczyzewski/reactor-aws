package com.ravenpack.aws.reactor.ddb.processor.generator;

import com.ravenpack.aws.reactor.ddb.processor.model.FieldDescription;

public class NotSupportedTypeException extends RuntimeException
{
    NotSupportedTypeException(FieldDescription fieldDescription)
    {
        super(fieldDescription.toString());
    }
}
