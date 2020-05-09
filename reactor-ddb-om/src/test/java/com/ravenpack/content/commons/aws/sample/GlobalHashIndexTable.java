package com.ravenpack.content.commons.aws.sample;

import com.ravenpack.content.commons.aws.reactor.ddb.datamodeling.DynamoDBHashKey;
import com.ravenpack.content.commons.aws.reactor.ddb.datamodeling.DynamoDBIndexHashKey;
import com.ravenpack.content.commons.aws.reactor.ddb.datamodeling.DynamoDBTable;
import lombok.Builder;
import lombok.Data;
import lombok.With;

@With
@Data
@Builder
@DynamoDBTable
public class GlobalHashIndexTable
{

    @DynamoDBHashKey
    private final String uid;

    @DynamoDBIndexHashKey(globalSecondaryIndexNames = { "globalSecondaryIndexName" })
    private final String globalId;

    private final String payload;

}
