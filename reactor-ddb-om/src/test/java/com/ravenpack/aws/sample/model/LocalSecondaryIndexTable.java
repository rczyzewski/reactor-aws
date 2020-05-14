package com.ravenpack.aws.sample.model;

import com.ravenpack.aws.reactor.ddb.datamodeling.DynamoDBHashKey;
import com.ravenpack.aws.reactor.ddb.datamodeling.DynamoDBLocalIndexRangeKey;
import com.ravenpack.aws.reactor.ddb.datamodeling.DynamoDBRangeKey;
import com.ravenpack.aws.reactor.ddb.datamodeling.DynamoDBTable;
import lombok.Builder;
import lombok.Data;
import lombok.Value;
import lombok.With;

@With
@Value
@Builder
@DynamoDBTable
public class LocalSecondaryIndexTable
{

    @DynamoDBHashKey
    String uid;

    @DynamoDBRangeKey
    String range;

    @DynamoDBLocalIndexRangeKey(localSecondaryIndexName = "secondRange")
    String secondRange;

    String payload;
}
