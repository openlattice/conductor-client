package com.openlattice.data.storage

import com.google.common.collect.Maps
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.query.Predicates
import com.openlattice.datastore.configuration.StorageConfiguration
import com.openlattice.hazelcast.HazelcastMap
import com.twilio.rest.authy.v1.service.EntityReader
import com.zaxxer.hikari.HikariDataSource
import java.util.*

/**
 * This class manages storage configurations for the system. For performance reasons, it builds up a static map
 * of readers and writers for each entity set. This class assumes that storage classes for an entity set will not
 * change without a separate synchronization and migration mechanism that quiesces reads, migrates data, and invalidates
 * the storage configuration on each datasseparate mechanism
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class StorageManagementService(
        hazelcastInstance: HazelcastInstance,
        val metastore: HikariDataSource
) {
    val entitySets = HazelcastMap.ENTITY_SETS.getMap(hazelcastInstance)
    val writers = Maps.newConcurrentMap<UUID, EntityWriter>() // entity set id ->  entity writer
    val readers = Maps.newConcurrentMap<UUID, EntityReader>() // entity set id -> entity reader
    val migrationReader = Maps.newConcurrentMap<UUID,EntityReader>() // entity set id -> entity reader
    private val storageConfigurations = HazelcastMap.STORAGE_CONFIGURATIONS.getMap(hazelcastInstance)

    fun getWriter(entitySetId: UUID): EntityWriter {
        return writers.getOrPut(entitySetId) {
            getStorageConfiguration(entitySetId).getWriter()
        }
    }

    //Push it to the reader

    fun getReader(entitySetId: UUID): EntityReader {
        return readers.getOrPut(entitySetId) {
            getStorageConfiguration(entitySetId).getLoader()
        }
    }

    /**
     * Retrieves a reader that allows
     */
    fun getMigrationReader(entitySetId: UUID) : EntityReader {

    }

    fun startMigrate( entitySetId: UUID, sourceDatastore: String, destinationDatastore : String ) {

    }
    /**
     * Returns whether an entity set is migrating or not.
     */
    fun isMigrating(entitySetId: UUID) : Boolean {

    }

    fun isMigrated( entitySetId: UUID, entityKeyIds: Set<UUID> ) : Map<UUID, Boolean> {

    }

    /*
     * When migrating all reads from both source and destination datastores
     *
     * All create / overwite calls write to destination datastore
     *
     * All replace calls, determine whether
     *
     * All delete calls propagate to both datastores
     */

    /**
     *
     */
    fun getStorage( entitySetId: UUID ) : String {

    }

    fun invalidateStorageConfiguration(entitySetId: UUID) {
        writers.remove(entitySetId)
        readers.remove(entitySetId)
    }

    fun getStorageConfiguration(entitySetId: UUID): StorageConfiguration = getStorageConfiguration(
            entitySets.getValue(entitySetId).storageType.name
    )

    fun getStorageConfiguration(name: String): StorageConfiguration = storageConfigurations.getValue(name)

    fun setStorageConfiguration(name: String, storageConfiguration: StorageConfiguration) {
        storageConfigurations.set(name, storageConfiguration)

        //TODO: Make this an indexed field and define as constant
        entitySets.values(Predicates.equal("storageName", name)).forEach { entitySet ->
            invalidateStorageConfiguration(entitySet.id)
        }
    }

    fun removeStorageConfiguration(name: String) = storageConfigurations.delete(name)

}

private fun