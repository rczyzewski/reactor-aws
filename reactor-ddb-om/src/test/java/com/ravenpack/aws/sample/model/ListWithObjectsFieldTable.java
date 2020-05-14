package com.ravenpack.aws.sample.model;

import com.ravenpack.aws.reactor.ddb.datamodeling.DynamoDBDocument;
import com.ravenpack.aws.reactor.ddb.datamodeling.DynamoDBHashKey;
import com.ravenpack.aws.reactor.ddb.datamodeling.DynamoDBTable;
import lombok.Builder;
import lombok.Value;
import lombok.With;

import java.util.List;

@DynamoDBTable
@Value
@Builder
@With
public class ListWithObjectsFieldTable {
    @DynamoDBHashKey
    String uid;

    List<InnerObject> payload;


    @With
    @Value
    @Builder
    @DynamoDBDocument
    public static class InnerObject{
        String name;
        String age;
    }
}


