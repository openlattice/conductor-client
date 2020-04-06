package com.openlattice.datastore.configuration

import com.hazelcast.core.HazelcastInstance
import com.hazelcast.core.IMap
import com.kryptnostic.rhizome.configuration.annotation.ReloadableConfiguration
import com.openlattice.data.EntityDataKey
import com.openlattice.data.integration.Entity
import com.openlattice.data.storage.ByteBlobDataManager
import com.openlattice.data.storage.EntityLoader
import com.openlattice.data.storage.EntityWriter
import com.openlattice.data.storage.aws.S3EntityDatastore
import com.openlattice.hazelcast.HazelcastMap
import javax.inject.Inject

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@ReloadableConfiguration(uri = "s3storage.yaml")
data class S3StorageConfiguration(
        val bucket: String,
        val region: String,
        val accessKeyId: String,
        val secretAccessKey: String,
        val threads: Int = 8
) : StorageConfiguration {
    @Inject
    lateinit var hazelcastInstance: HazelcastInstance

    override fun getLoader(byteBlobDataManager: ByteBlobDataManager): EntityLoader {
        return getS3EntityDatastore(byteBlobDataManager)
    }

    override fun getWriter(byteBlobDataManager: ByteBlobDataManager): EntityWriter {
        return getS3EntityDatastore(byteBlobDataManager)
    }

    private fun getS3EntityDatastore(
            byteBlobDataManager: ByteBlobDataManager
    ): S3EntityDatastore {
        return S3EntityDatastore(this, byteBlobDataManager, HazelcastMap.S3_OBJECT_STORE.getMap(hazelcastInstance))
    }
}