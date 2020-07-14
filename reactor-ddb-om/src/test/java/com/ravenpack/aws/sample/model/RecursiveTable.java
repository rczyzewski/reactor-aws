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
public class RecursiveTable {


    @DynamoDBHashKey
    String uid;

    RecursiveTable data;

    String payload;
}
