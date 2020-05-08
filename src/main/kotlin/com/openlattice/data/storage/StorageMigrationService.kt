package com.openlattice.data.storage

import com.hazelcast.core.HazelcastInstance
import com.openlattice.controllers.exceptions.ResourceNotFoundException
import com.openlattice.edm.type.PropertyType
import com.openlattice.hazelcast.HazelcastMap
import java.util.*

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

            val entity = oldReader.getHistoricalEntitiesById(entityKeyIds, getPropertyTypesForEntitySet(entitySetId))

        }

    }

    fun getPropertyTypesForEntitySet(entitySetId: UUID): Map<UUID, PropertyType> {

    }
}

data class MigrationStatus(
        val entitySetId: UUID,
        val oldDatastore: String,
        val newDatastore: String,
        val migratedCount: Long,
        val remaining: Long
)