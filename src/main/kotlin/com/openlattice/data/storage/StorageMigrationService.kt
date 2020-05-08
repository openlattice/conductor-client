package com.openlattice.data.storage

import com.hazelcast.core.HazelcastInstance
import com.openlattice.controllers.exceptions.ResourceNotFoundException
import com.openlattice.data.Property
import com.openlattice.edm.type.PropertyType
import com.openlattice.hazelcast.HazelcastMap
import java.nio.ByteBuffer
import java.util.*
import kotlin.math.abs

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class StorageMigrationService(
        hazelcastInstance: HazelcastInstance,
        val storageManagementService: StorageManagementService
) {
    private val migrationStatus = HazelcastMap.MIGRATION_STATUS.getMap(hazelcastInstance)

    /**
     * Start a migration to a new datastore.
     *
     * @param entitySetId The entity set id of the entity set to migrate.
     * @param datastore The new datastore to which all entities will be migrated.
     */
    fun startMigration(entitySetId: UUID, datastore: String) {

    }

    fun getMigrationStatus(entitySetId: UUID): MigrationStatus = migrationStatus[entitySetId]
            ?: throw ResourceNotFoundException("Unable to find migrating entity set.")

    fun isMigrating(entitySetId: UUID): Boolean = migrationStatus.containsKey(entitySetId)

    fun getMigrationState(entityDataKeys: Map<UUID, Set<UUID>>) {
        //Figure out which keys need to be migrated and migrate them.
        //Avoid checking
    }

    /**
     * Automatically loads
     */
    fun migrateIfNeeded(entityKeyIds: Map<UUID, Optional<Set<UUID>>>) {
        //Retrieve the migration status corresponding to entity sets
        val migrating = migrationStatus.getAll(entityKeyIds.keys)

        //For entity sets that are migrating
        migrating.forEach { entitySetId, migrationStatus ->
            val oldReader = storageManagementService.getReader(migrationStatus.oldDatastore)
            val newWriter = storageManagementService.getWriter(migrationStatus.newDatastore)

            val entities = oldReader.getHistoricalEntitiesById(entityKeyIds, getPropertyTypesForEntitySet(entitySetId))
            val allVersions = getAllVersionsOfEntities(entities)
        }

    }

    fun getPropertyTypesForEntitySet(entitySetId: UUID): Map<UUID, PropertyType> {

    }

    /**
     * This function takes the entire history of an entity and generates every version of an object.
     */
    fun getAllVersionsOfEntities(
            entities: Map<UUID, MutableMap<UUID, MutableMap<ByteBuffer, Property>>>
    ): Map<UUID, Map<UUID, MutableMap<Long, MutableMap<UUID, MutableSet<Any>>>>> {
        return entities.mapValues { (entitySetId, entities) ->
            entities.mapValues { (entityKeyId, properties) ->
                getAllVersionsOfEntity(properties)
            }
        }
    }

    fun getAllVersionsOfEntity(
            properties: MutableMap<ByteBuffer, Property>
    ): MutableMap<Long, MutableMap<UUID, MutableSet<Any>>> {
        //Build the list of unique versions for this object
        val allVersions = properties.values.flatMap { it.versions.get().asIterable() }.toSet()

        allVersions.associateBy { version ->
            toEntity(properties, version)
        }
        return mutableMapOf()
    }

    private fun toEntity(properties: MutableMap<ByteBuffer, Property>, version: Long) {
        val entity = mutableMapOf<UUID, MutableSet<Any>>()
        properties.values.filter { property ->
            property.versions.get().filter { propertyVersion ->
                abs(
                        propertyVersion
                ) < version
            }.maxBy { propertyVersion -> abs(propertyVersion) }!! > 0
        }
    }


}

data class MigrationStatus(
        val entitySetId: UUID,
        val oldDatastore: String,
        val newDatastore: String,
        val migratedCount: Long,
        val remaining: Long
)