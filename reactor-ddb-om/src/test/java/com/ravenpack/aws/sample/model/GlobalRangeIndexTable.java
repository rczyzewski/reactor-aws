package com.ravenpack.aws.sample.model;

import com.ravenpack.aws.reactor.ddb.datamodeling.DynamoDBHashKey;
import com.ravenpack.aws.reactor.ddb.datamodeling.DynamoDBIndexHashKey;
import com.ravenpack.aws.reactor.ddb.datamodeling.DynamoDBIndexRangeKey;
import com.ravenpack.aws.reactor.ddb.datamodeling.DynamoDBTable;
import lombok.Builder;
import lombok.Data;
import lombok.Value;
import lombok.With;

@With
@Value
@Builder
@DynamoDBTable
public class GlobalRangeIndexTable
{

    @DynamoDBHashKey
    String uid;

    @DynamoDBIndexHashKey(globalSecondaryIndexNames = { "globalSecondaryIndexName" })
    String globalId;

    @DynamoDBIndexRangeKey(globalSecondaryIndexNames = { "globalSecondaryIndexName" })
    String globalRange;

    String payload;
}
