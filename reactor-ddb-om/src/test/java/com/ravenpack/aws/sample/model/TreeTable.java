package com.ravenpack.aws.sample.model;

import com.ravenpack.aws.reactor.ddb.datamodeling.DynamoDBDocument;
import com.ravenpack.aws.reactor.ddb.datamodeling.DynamoDBHashKey;
import com.ravenpack.aws.reactor.ddb.datamodeling.DynamoDBTable;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;
import lombok.With;

import java.util.List;

@DynamoDBTable
@Value
@Builder
@With
public class TreeTable {

    @DynamoDBHashKey
    String uid;


    TreeBranch content;

    @With
    @Value
    @Builder

    @DynamoDBDocument
    public static class TreeBranch
    {
        @Singular
        List<TreeBranch> subbranches;
        String payload;
    }


}
