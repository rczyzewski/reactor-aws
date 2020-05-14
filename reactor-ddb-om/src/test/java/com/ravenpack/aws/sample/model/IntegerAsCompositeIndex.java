package com.ravenpack.aws.sample.model;

import com.ravenpack.aws.reactor.ddb.datamodeling.DynamoDBHashKey;
import com.ravenpack.aws.reactor.ddb.datamodeling.DynamoDBRangeKey;
import com.ravenpack.aws.reactor.ddb.datamodeling.DynamoDBTable;
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
