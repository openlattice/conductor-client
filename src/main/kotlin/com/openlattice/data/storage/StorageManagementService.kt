package com.openlattice.data.storage

import com.google.common.collect.Maps
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.query.Predicates
import com.openlattice.datastore.configuration.StorageConfiguration
import com.openlattice.hazelcast.HazelcastMap
import com.zaxxer.hikari.HikariDataSource
import java.util.*

/**
 * This class manages storage configurations for the system. For performance reasons, it builds up a static map
 * of readers and writers for each entity set. This class assumes that storage classes for an entity set will not
 * change without a separate synchronization and migration mechanism that quiesces reads, migrates data, and invalidates
 * the storage configuration on each data
 *
 * This assumes that the migration service migrats
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class StorageManagementService(
        hazelcastInstance: HazelcastInstance,
        val metastore: HikariDataSource
) {
    private val entitySets = HazelcastMap.ENTITY_SETS.getMap(hazelcastInstance)
    private val writers = Maps.newConcurrentMap<String, EntityWriter>() // datastore name ->  entity writer
    private val readers = Maps.newConcurrentMap<String, EntityLoader>() // datastore name -> entity reader

    private val storageConfigurations = HazelcastMap.STORAGE_CONFIGURATIONS.getMap(hazelcastInstance)

    init {
        storageConfigurations.forEach { (name, storageConfiguration) ->
            writers[name] = storageConfiguration.getWriter()
            readers[name] = storageConfiguration.getLoader()
        }
    }

    fun getWriter(entitySetId: UUID): EntityWriter = writers.getValue(getStorage(entitySetId))
    fun getWriter(name: String): EntityWriter = writers.getValue(name)

    //Push it to the reader
    fun getReader(entitySetId: UUID): EntityLoader = readers.getValue(getStorage(entitySetId))
    fun getReader(name: String): EntityLoader = readers.getValue(name)


    /*
     * For all operations trigger a migration for data. Process all operations against new data store.
     *
     * All delete calls propagate to both datastores
     */

    /**
     *
     */
    fun getStorage(entitySetId: UUID): String = entitySets.getValue(entitySetId).storageType.name

    fun getStorageConfiguration(entitySetId: UUID): StorageConfiguration = getStorageConfiguration(
            entitySets.getValue(entitySetId).storageType.name
    )

    fun getStorageConfiguration(name: String): StorageConfiguration = storageConfigurations.getValue(name)

    fun setStorageConfiguration(name: String, storageConfiguration: StorageConfiguration) {
        storageConfigurations.set(name, storageConfiguration)
    }

    fun removeStorageConfiguration(name: String) = storageConfigurations.delete(name)
}

private fun