package com.openlattice.data.storage.aws

import com.amazonaws.retry.PredefinedBackoffStrategies
import com.amazonaws.retry.PredefinedRetryPolicies
import com.amazonaws.retry.RetryPolicy
import com.amazonaws.services.s3.AmazonS3
import com.geekbeast.rhizome.aws.newS3Client
import com.openlattice.datastore.configuration.DatastoreConfiguration


/**
 * Creates a new S3 client from a datastore configuration
 */
internal fun newS3Client(datastoreConfiguration: DatastoreConfiguration): AmazonS3 {
    return newS3Client(
            datastoreConfiguration.accessKeyId,
            datastoreConfiguration.secretAccessKey,
            datastoreConfiguration.regionName,
            RetryPolicy(
                    PredefinedRetryPolicies.DEFAULT_RETRY_CONDITION,
                    PredefinedBackoffStrategies.SDKDefaultBackoffStrategy(), //TODO try jitter
                    MAX_ERROR_RETRIES,
                    false
            )
    )
}
