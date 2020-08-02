package com.openlattice.data.storage.providers

import com.openlattice.data.storage.EntityLoader
import com.openlattice.data.storage.EntityWriter
import com.openlattice.data.storage.StorageConfiguration
import java.util.function.Supplier

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
interface StorageProvider {
    val entityLoader: EntityLoader
    val entityWriter: EntityWriter
    val storageConfiguration: StorageConfiguration
}