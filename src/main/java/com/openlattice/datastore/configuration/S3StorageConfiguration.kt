package com.openlattice.datastore.configuration

import com.openlattice.data.storage.EntityLoader
import com.openlattice.data.storage.EntityWriter
import com.openlattice.data.storage.aws.S3EntityDatastore

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
data class S3StorageConfiguration(
        val bucket : String,
        val region : String,
        val accessKeyId: String,
        val secretAccessKey: String,
        val threads: Int = 8
) : StorageConfiguration {
    override fun getLoader(): EntityLoader {
        return getS3EntityDatastore()
    }

    override fun getWriter(): EntityWriter {
        return getS3EntityDatastore()
    }

    private fun getS3EntityDatastore() : S3EntityDatastore {
        return S3EntityDatastore(this)
    }
}