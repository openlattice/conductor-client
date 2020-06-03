package com.openlattice.datastore.configuration

import com.codahale.metrics.MetricRegistry
import com.hazelcast.core.HazelcastInstance
import com.kryptnostic.rhizome.configuration.annotation.ReloadableConfiguration
import com.openlattice.data.storage.ByteBlobDataManager
import com.openlattice.data.storage.EntityLoader
import com.openlattice.data.storage.EntityWriter
import com.openlattice.data.storage.StorageConfiguration
import com.openlattice.data.storage.aws.S3EntityDatastore
import com.openlattice.hazelcast.HazelcastMap
import javax.inject.Inject

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
data class S3StorageProvider(
        private val hazelcastInstance: HazelcastInstance,
        private val byteBlobDataManager: ByteBlobDataManager,
        private val metricRegistry: MetricRegistry
) : StorageProvider {
    override val entityLoader = getLoader()
    override val entityWriter = getWriter()

    private fun getLoader(): EntityLoader {
        return getS3EntityDatastore(byteBlobDataManager)
    }

    private fun getWriter(): EntityWriter {
        return getS3EntityDatastore(byteBlobDataManager)
    }

    private fun getS3EntityDatastore(
            byteBlobDataManager: ByteBlobDataManager
    ): S3EntityDatastore {
        return S3EntityDatastore(
                this, byteBlobDataManager,
                HazelcastMap.S3_OBJECT_STORE.getMap(hazelcastInstance),
                metricRegistry
        )
    }
}