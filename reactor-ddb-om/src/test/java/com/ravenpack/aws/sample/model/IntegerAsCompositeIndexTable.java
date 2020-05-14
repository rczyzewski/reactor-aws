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
public class IntegerAsCompositeIndexTable
{

    @DynamoDBHashKey
    Integer uid;

    @DynamoDBRangeKey
    Integer range;

    String payload;

    Integer val;

    Double fuzzyVal;
}
