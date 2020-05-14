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
public class CompositePrimaryIndexTable
{
    @DynamoDBHashKey
    private final String uid;

    @DynamoDBRangeKey
    private final String range;

    private final String payload;

    private final Integer val;

    private final Double fuzzyVal;
}
