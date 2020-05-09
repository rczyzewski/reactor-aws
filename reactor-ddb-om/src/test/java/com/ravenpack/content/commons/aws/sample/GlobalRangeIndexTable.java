package com.ravenpack.content.commons.aws.sample;

import com.ravenpack.content.commons.aws.reactor.ddb.datamodeling.DynamoDBHashKey;
import com.ravenpack.content.commons.aws.reactor.ddb.datamodeling.DynamoDBIndexHashKey;
import com.ravenpack.content.commons.aws.reactor.ddb.datamodeling.DynamoDBIndexRangeKey;
import com.ravenpack.content.commons.aws.reactor.ddb.datamodeling.DynamoDBTable;
import lombok.Builder;
import lombok.Data;
import lombok.With;

@With
@Data
@Builder
@DynamoDBTable
public class GlobalRangeIndexTable
{

    @DynamoDBHashKey
    private final String uid;

    @DynamoDBIndexHashKey(globalSecondaryIndexNames = { "globalSecondaryIndexName" })
    private final String globalId;

    @DynamoDBIndexRangeKey(globalSecondaryIndexNames = { "globalSecondaryIndexName" })
    private final String globalRange;

    private final String payload;
}
