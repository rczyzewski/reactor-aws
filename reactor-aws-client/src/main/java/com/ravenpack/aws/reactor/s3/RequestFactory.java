package com.ravenpack.aws.reactor.s3;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.jetbrains.annotations.NotNull;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
final class RequestFactory
{

    static CreateMultipartUploadRequest createMultipartUploadRequest(
        String bucket,
        String key)
    {
        return CreateMultipartUploadRequest.builder()
            .bucket(bucket).key(key)
            .serverSideEncryption(ServerSideEncryption.AES256)
            .build();
    }

    static CreateMultipartUploadRequest createMultipartUploadRequest(
        String bucket,
        String key,
        @NotNull String contentType)
    {
        return CreateMultipartUploadRequest.builder()
            .bucket(bucket).key(key)
            .contentType(contentType)
            .serverSideEncryption(ServerSideEncryption.AES256)
            .build();
    }

    static UploadPartRequest createUploadPartRequest(
        CreateMultipartUploadResponse upload, byte[] bytes,
        int partNumber)
    {
        return UploadPartRequest.builder()
            .bucket(upload.bucket())
            .contentLength((long) bytes.length)
            .uploadId(upload.uploadId())
            .partNumber(partNumber)
            .key(upload.key())
            .build();
    }

    static CompletedPart prepareCompletePart(RxS3Impl.PartUploadTuple responseTuple)
    {
        return CompletedPart.builder()
            .partNumber(responseTuple.getPartNumber())
            .eTag(responseTuple.getPartUploadResponse().eTag())
            .build();
    }

    static CompleteMultipartUploadRequest prepareCompleteMultipartUploadRequest(
        CreateMultipartUploadResponse upload,
        CompletedMultipartUpload completedMultipartUpload)
    {
        return CompleteMultipartUploadRequest.builder()
            .bucket(upload.bucket())
            .key(upload.key())
            .uploadId(upload.uploadId())
            .multipartUpload(completedMultipartUpload)
            .build();
    }

    static AbortMultipartUploadRequest createAbortMultipartUploadRequest(CreateMultipartUploadResponse upload)
    {
        return AbortMultipartUploadRequest.builder()
            .bucket(upload.bucket())
            .key(upload.key())
            .uploadId(upload.uploadId())
            .build();
    }
}
