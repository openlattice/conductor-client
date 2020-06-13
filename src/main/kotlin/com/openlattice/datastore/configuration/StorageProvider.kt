package com.openlattice.datastore.configuration

import com.openlattice.data.storage.EntityLoader
import com.openlattice.data.storage.EntityWriter
import com.openlattice.data.storage.StorageConfiguration
import java.util.function.Supplier

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
interface StorageProvider {
    val entityLoader: Supplier<EntityLoader>
    val entityWriter: Supplier<EntityWriter>
    val storageConfiguration: StorageConfiguration
}