package com.openlattice.aws

import com.amazonaws.services.s3.AmazonS3
import com.geekbeast.rhizome.aws.newS3Client

fun newS3Client(s3ClientConfiguration: AwsS3ClientConfiguration): AmazonS3 {
    return newS3Client(
            s3ClientConfiguration.accessKeyId,
            s3ClientConfiguration.secretAccessKey,
            s3ClientConfiguration.regionName
    )
}