package com.openlattice.datastore.configuration

import com.openlattice.data.storage.EntityLoader
import com.openlattice.data.storage.EntityWriter
import com.openlattice.data.storage.StorageConfiguration

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
interface StorageProvider {
    val entityLoader: EntityLoader
    val entityWriter: EntityWriter
    val storageConfiguration: StorageConfiguration
}