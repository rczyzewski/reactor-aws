package com.ravenpack.aws.sample.model;

import com.ravenpack.aws.reactor.ddb.datamodeling.DynamoDBHashKey;
import com.ravenpack.aws.reactor.ddb.datamodeling.DynamoDBLocalIndexRangeKey;
import com.ravenpack.aws.reactor.ddb.datamodeling.DynamoDBRangeKey;
import com.ravenpack.aws.reactor.ddb.datamodeling.DynamoDBTable;
import lombok.Builder;
import lombok.Data;
import lombok.With;

@With
@Data
@Builder
@DynamoDBTable
public class LocalSecondaryIndexTable
{

    @DynamoDBHashKey
    private final String uid;

    @DynamoDBRangeKey
    private final String range;

    @DynamoDBLocalIndexRangeKey(localSecondaryIndexName = "secondRange")
    private final String secondRange;

    private final String payload;
}
