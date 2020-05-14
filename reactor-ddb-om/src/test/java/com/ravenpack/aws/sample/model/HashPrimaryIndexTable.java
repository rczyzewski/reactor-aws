package com.ravenpack.aws.sample.model;

import com.ravenpack.aws.reactor.ddb.datamodeling.DynamoDBHashKey;
import com.ravenpack.aws.reactor.ddb.datamodeling.DynamoDBTable;
import lombok.Builder;
import lombok.Value;
import lombok.With;

@DynamoDBTable
@Value
@Builder
@With
public class HashPrimaryIndexTable
{

    @DynamoDBHashKey
    private final String uid;

    private final String payload;
}
