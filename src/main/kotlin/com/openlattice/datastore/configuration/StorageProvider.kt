package com.openlattice.datastore.configuration

import com.openlattice.data.storage.EntityLoader
import com.openlattice.data.storage.EntityWriter

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
interface StorageProvider {
    val entityLoader: EntityLoader
    val entityWriter: EntityWriter
    
}