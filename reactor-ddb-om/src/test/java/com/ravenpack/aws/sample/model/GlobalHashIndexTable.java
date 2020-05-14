package com.ravenpack.aws.sample.model;

import com.ravenpack.aws.reactor.ddb.datamodeling.DynamoDBHashKey;
import com.ravenpack.aws.reactor.ddb.datamodeling.DynamoDBIndexHashKey;
import com.ravenpack.aws.reactor.ddb.datamodeling.DynamoDBTable;
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
