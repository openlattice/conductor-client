package com.openlattice.data.storage

import com.hazelcast.core.HazelcastInstance
import com.openlattice.controllers.exceptions.ResourceNotFoundException
import com.openlattice.data.Property
import com.openlattice.datastore.services.EntitySetService
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
        val storageManagementService: StorageManagementService,
        val entitySetService: EntitySetService
) {
    private val migrationStatus = HazelcastMap.MIGRATION_STATUS.getMap(hazelcastInstance)
    private var migratingEntitySets = migrationStatus.keys

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

    fun getEntitiesNeedingMigration(entityKeyIds: Map<UUID, Optional<Set<UUID>>>): Map<UUID, Set<UUID>> {
        //Query postgres to see where 
    }

    /**
     * Migrates data to new datastore so read can proceed as intended.
     */
    fun migrateIfNeeded(entityKeyIds: Map<UUID, Optional<Set<UUID>>>) {
        val entitySetsNeedingMigration = entityKeyIds.keys.intersect(migratingEntitySets)

        //Exit quickly if no entity sets need migration.
        if (entitySetsNeedingMigration.isEmpty()) {
            return
        }

        //Retrieve the list of entities in the read that still need migration.
        val entitiesNeedingMigration = getEntitiesNeedingMigration(entityKeyIds)
        val propertyTypes = entitySetService.getPropertyTypesOfEntitySets(entitySetsNeedingMigration)

        //Retrieve the migration status corresponding to entity sets
        val migrating = migrationStatus.getAll(entitiesNeedingMigration.keys)

        //Migrate entities that are needed to successfully complete the write.
        //We have to go entity set, by entity set, because each one might be doing a potentially different migration
        migrating.forEach { (entitySetId, migrationStatus) ->
            val oldReader = storageManagementService.getReader(migrationStatus.oldDatastore)
            val newWriter = storageManagementService.getWriter(migrationStatus.newDatastore)

            val entities = oldReader.getHistoricalEntitiesById(
                    mapOf(entitySetId to Optional.of(entitiesNeedingMigration.getValue(entitySetId))),
                    propertyTypes
            )
            newWriter.writeEntitiesWithHistory(entities)
        }
    }


    fun updateMigratingEntitySets() {
        migratingEntitySets = migrationStatus.keys
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