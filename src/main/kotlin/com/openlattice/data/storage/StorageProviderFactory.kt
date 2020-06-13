package com.openlattice.data.storage

import com.codahale.metrics.MetricRegistry
import com.hazelcast.core.HazelcastInstance
import com.openlattice.datastore.configuration.S3StorageProvider
import com.openlattice.datastore.configuration.StorageProvider
import java.security.InvalidParameterException

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class StorageProviderFactory(
        private val byteBlobDataManager: ByteBlobDataManager,
        private val metricRegistry: MetricRegistry
) {
    lateinit var hazelcastInstance: HazelcastInstance
    fun buildStorageProvider(storageConfiguration: StorageConfiguration): StorageProvider {
        return when (storageConfiguration) {
//            is PostgresStorageConfiguration ->
            is S3StorageConfiguration -> S3StorageProvider(
                    hazelcastInstance,
                    byteBlobDataManager,
                    metricRegistry,
                    storageConfiguration
            )
            else -> throw InvalidParameterException("Unrecognized storage configuration class.")
        }
    }
}