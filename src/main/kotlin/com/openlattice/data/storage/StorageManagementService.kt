package com.openlattice.data.storage

import com.hazelcast.core.HazelcastInstance
import com.openlattice.datastore.configuration.StorageProvider
import com.openlattice.hazelcast.HazelcastMap
import com.zaxxer.hikari.HikariDataSource
import java.util.*

/**
 * This class manages storage configurations for the system. For performance reasons, it builds up a static map
 * of readers and writers for each entity set. This class assumes that storage classes for an entity set will not
 * change without a separate synchronization and migration mechanism that quiesces reads, migrates data, and invalidates
 * the storage configuration on each data
 *
 * This assumes that the migration service migrates data
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class StorageManagementService(
        hazelcastInstance: HazelcastInstance,
        val metastore: HikariDataSource
) {
    private val entitySets = HazelcastMap.ENTITY_SETS.getMap(hazelcastInstance)
    private val storageProviders = HazelcastMap.STORAGE_PROVIDERS.getMap(hazelcastInstance)

    fun getWriter(entitySetId: UUID): EntityWriter = getWriter(getStorage(entitySetId))
    fun getWriter(name: String): EntityWriter = storageProviders.getValue(name).entityWriter

    //Push it to the reader
    fun getReader(entitySetId: UUID): EntityLoader = getReader(getStorage(entitySetId))
    fun getReader(name: String): EntityLoader = storageProviders.getValue(name).entityLoader


    /*
     * For all operations trigger a migration for data. Process all operations against new data store.
     *
     * All delete calls propagate to both datastores
     */

    /**
     *
     */
    fun getStorage(entitySetId: UUID): String = entitySets.getValue(entitySetId).storageType.name

    fun getStorageConfiguration(entitySetId: UUID): StorageProvider = getStorageConfiguration(
            entitySets.getValue(entitySetId).storageType.name
    )

    fun getStorageConfiguration(name: String): StorageProvider = storageProviders.getValue(name)

    fun setStorageConfiguration(name: String, storageConfiguration: StorageConfiguration) {
        storageProviders.set(name, storageConfiguration)
    }

    fun removeStorageConfiguration(name: String) = storageProviders.delete(name)
}

