package com.ravenpack.content.commons.aws.sample;

import com.ravenpack.content.commons.aws.reactor.ddb.datamodeling.DynamoDBHashKey;
import com.ravenpack.content.commons.aws.reactor.ddb.datamodeling.DynamoDBRangeKey;
import com.ravenpack.content.commons.aws.reactor.ddb.datamodeling.DynamoDBTable;
import lombok.Builder;
import lombok.Value;
import lombok.With;

@With
@Value
@Builder
@DynamoDBTable
public class IntegerAsCompositeIndex
{

    @DynamoDBHashKey
    private final Integer uid;

    @DynamoDBRangeKey
    private final Integer range;

    private final String payload;

    private final Integer val;

    private final Double fuzzyVal;
}
