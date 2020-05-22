package com.openlattice.data.storage

import com.codahale.metrics.MetricRegistry
import com.codahale.metrics.Timer
import com.codahale.metrics.annotation.Timed
import com.google.common.collect.ImmutableMap
import com.google.common.eventbus.EventBus
import com.openlattice.assembler.events.MaterializedEntitySetDataChangeEvent
import com.openlattice.data.DeleteType
import com.openlattice.data.Property
import com.openlattice.data.WriteEvent
import com.openlattice.data.events.EntitiesDeletedEvent
import com.openlattice.data.events.EntitiesUpsertedEvent
import com.openlattice.datastore.services.EntitySetManager
import com.openlattice.edm.PostgresEdmManager
import com.openlattice.edm.events.EntitySetDataDeletedEvent
import com.openlattice.edm.set.EntitySetFlag
import com.openlattice.edm.type.PropertyType
import com.openlattice.linking.LinkingQueryService
import com.openlattice.linking.PostgresLinkingFeedbackService
import com.openlattice.postgres.streams.BasePostgresIterable
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.nio.ByteBuffer
import java.util.*
import java.util.stream.Stream
import javax.inject.Inject


/**
 *
 * Manages CRUD for entities and entity sets in the system.
 */
@Service
class PostgresEntityDatastore(
        private val dataQueryService: PostgresEntityDataQueryService,
        private val postgresEdmManager: PostgresEdmManager,
        private val entitySetManager: EntitySetManager,
        private val metricRegistry: MetricRegistry,
        private val eventBus: EventBus,
        private val feedbackQueryService: PostgresLinkingFeedbackService,
        private val linkingQueryService: LinkingQueryService
) : EntityDatastore {

    companion object {
        private val logger = LoggerFactory.getLogger(PostgresEntityDatastore::class.java)
        const val BATCH_INDEX_THRESHOLD = 256
    }

    override val entitiesTimer: Timer = metricRegistry.timer(
            MetricRegistry.name(
                    PostgresEntityDatastore::class.java, "getEntities"
            )
    )
    override val linkedEntitiesTimer: Timer = metricRegistry.timer(
            MetricRegistry.name(
                    PostgresEntityDatastore::class.java, "getEntities(linked)"
            )
    )

    @Timed
    override fun createOrUpdateEntities(
            entitySetId: UUID,
            entities: Map<UUID, MutableMap<UUID, MutableSet<Any>>>,
            authorizedPropertyTypes: Map<UUID, PropertyType>,
            versioned: Boolean
    ): WriteEvent {
        // need to collect linking ids before writes to the entities

        val writeEvent = dataQueryService.upsertEntities(entitySetId, entities, authorizedPropertyTypes)
        signalCreatedEntities(entitySetId, entities.keys)

        return writeEvent
    }

    @Timed
    override fun replaceEntities(
            entitySetId: UUID,
            entities: Map<UUID, MutableMap<UUID, MutableSet<Any>>>,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): WriteEvent {
        // need to collect linking ids before writes to the entities

        val writeEvent = dataQueryService.replaceEntities(entitySetId, entities, authorizedPropertyTypes)
        signalCreatedEntities(entitySetId, entities.keys)

        return writeEvent
    }

    @Timed
    override fun partialReplaceEntities(
            entitySetId: UUID,
            entities: Map<UUID, MutableMap<UUID, MutableSet<Any>>>,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): WriteEvent {
        // need to collect linking ids before writes to the entities
        val writeEvent = dataQueryService
                .partialReplaceEntities(entitySetId, entities, authorizedPropertyTypes)

        signalCreatedEntities(entitySetId, entities.keys)

        return writeEvent
    }

    private fun signalCreatedEntities(entitySetId: UUID, entityKeyIds: Set<UUID>) {
        if (shouldIndexDirectly(entitySetId, entityKeyIds)) {
            val entities = dataQueryService
                    .getEntitiesWithPropertyTypeIds(
                            ImmutableMap.of(entitySetId, Optional.of(entityKeyIds)),
                            ImmutableMap.of(entitySetId, entitySetManager.getPropertyTypesForEntitySet(entitySetId)),
                            mapOf(),
                            EnumSet.of(MetadataOption.LAST_WRITE)
                    )
            eventBus.post(EntitiesUpsertedEvent(entitySetId, entities.toMap()))
        }

        markMaterializedEntitySetDirty(entitySetId) // mark entityset as unsync with data
        // mark all involved linking entitysets as unsync with data
        postgresEdmManager.getAllLinkingEntitySetIdsForEntitySet(entitySetId)
                .forEach { this.markMaterializedEntitySetDirty(it) }
    }

    private fun signalEntitySetDataDeleted(entitySetId: UUID, deleteType: DeleteType) {
        eventBus.post(EntitySetDataDeletedEvent(entitySetId, deleteType))
        markMaterializedEntitySetDirty(entitySetId) // mark entityset as unsync with data

        // mark all involved linking entitysets as unsync with data
        postgresEdmManager.getAllLinkingEntitySetIdsForEntitySet(entitySetId)
                .forEach { this.markMaterializedEntitySetDirty(it) }
    }

    private fun signalDeletedEntities(entitySetId: UUID, entityKeyIds: Set<UUID>, deleteType: DeleteType) {
        if (shouldIndexDirectly(entitySetId, entityKeyIds)) {
            eventBus.post(EntitiesDeletedEvent(entitySetId, entityKeyIds, deleteType))
        }

        markMaterializedEntitySetDirty(entitySetId) // mark entityset as unsync with data

        // mark all involved linking entitysets as unsync with data
        postgresEdmManager.getAllLinkingEntitySetIdsForEntitySet(entitySetId)
                .forEach { this.markMaterializedEntitySetDirty(it) }
    }

    private fun shouldIndexDirectly(entitySetId: UUID, entityKeyIds: Set<UUID>): Boolean {
        return entityKeyIds.size < BATCH_INDEX_THRESHOLD
                && entitySetManager.getEntitySetIdsWithFlags(setOf(entitySetId), setOf(EntitySetFlag.AUDIT)).isEmpty()
    }

    private fun markMaterializedEntitySetDirty(entitySetId: UUID) {
        eventBus.post(MaterializedEntitySetDataChangeEvent(entitySetId))
    }

    @Timed
    override fun replacePropertiesInEntities(
            entitySetId: UUID,
            replacementProperties: Map<UUID, Map<UUID, Set<Map<ByteBuffer, Any>>>>,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): WriteEvent {

        val writeEvent = dataQueryService
                .replacePropertiesInEntities(entitySetId, replacementProperties, authorizedPropertyTypes)
        signalCreatedEntities(entitySetId, replacementProperties.keys)

        return writeEvent
    }

    @Timed
    override fun clearEntitySet(
            entitySetId: UUID, authorizedPropertyTypes: Map<UUID, PropertyType>
    ): WriteEvent {
        val writeEvent = dataQueryService.clearEntitySet(entitySetId, authorizedPropertyTypes)
        signalEntitySetDataDeleted(entitySetId, DeleteType.Soft)
        return writeEvent
    }

    @Timed
    override fun clearEntities(
            entitySetId: UUID, entityKeyIds: Set<UUID>, authorizedPropertyTypes: Map<UUID, PropertyType>
    ): WriteEvent {
        val writeEvent = dataQueryService.clearEntities(entitySetId, entityKeyIds, authorizedPropertyTypes)
        signalDeletedEntities(entitySetId, entityKeyIds, DeleteType.Soft)
        return writeEvent
    }

    @Timed
    override fun clearEntityProperties(
            entitySetId: UUID,
            entityKeyIds: Set<UUID>,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): WriteEvent {
        val writeEvent = dataQueryService.clearEntityData(entitySetId, entityKeyIds, authorizedPropertyTypes)
        // same as if we updated the entities
        signalCreatedEntities(entitySetId, entityKeyIds)

        return writeEvent
    }

    @Timed
    override fun getAllEntitiesWithMetadata(
            entitySetId: UUID,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
            metadataOptions: EnumSet<MetadataOption>
    ): Stream<MutableMap<FullQualifiedName, MutableSet<Any>>> {
        return getEntitiesWithMetadataInternal(entitySetId, emptySet(), authorizedPropertyTypes, metadataOptions)
    }

    @Timed
    override fun getEntitiesWithMetadata(
            entitySetId: UUID,
            ids: Set<UUID>,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
            metadataOptions: EnumSet<MetadataOption>
    ): Stream<MutableMap<FullQualifiedName, MutableSet<Any>>> {
        return if (ids.isEmpty()) {
            Stream.of()
        } else {
            getEntitiesWithMetadataInternal(entitySetId, ids, authorizedPropertyTypes, metadataOptions)
        }
    }

    private fun getEntitiesWithMetadataInternal(
            entitySetId: UUID,
            ids: Set<UUID>,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
            metadataOptions: EnumSet<MetadataOption>
    ): Stream<MutableMap<FullQualifiedName, MutableSet<Any>>> {
        //If the query generated exceeds 33.5M UUIDs good chance that it exceeds Postgres's 1 GB max query buffer size

        return dataQueryService.getEntitiesWithPropertyTypeFqns(
                ImmutableMap.of(entitySetId, Optional.of(ids)),
                authorizedPropertyTypes,
                emptyMap(),
                metadataOptions
        ).values.stream()
    }

    @Timed
    override fun getLinkingEntities(
            entityKeyIds: Map<UUID, Optional<Set<UUID>>>,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>
    ): Stream<MutableMap<FullQualifiedName, MutableSet<Any>>> {
        //If the query generated exceed 33.5M UUIDs good chance that it exceed Postgres's 1 GB max query buffer size
        return getLinkingEntitiesWithMetadata(
                entityKeyIds,
                authorizedPropertyTypes,
                EnumSet.noneOf(MetadataOption::class.java)
        )
    }

    @Timed
    override fun getLinkingEntitiesWithMetadata(
            entityKeyIds: Map<UUID, Optional<Set<UUID>>>,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
            metadataOptions: EnumSet<MetadataOption>
    ): Stream<MutableMap<FullQualifiedName, MutableSet<Any>>> {
        //If the query generated exceed 33.5M UUIDs good chance that it exceed Postgres's 1 GB max query buffer size
        return dataQueryService.getEntitiesWithPropertyTypeFqns(
                entityKeyIds,
                authorizedPropertyTypes,
                emptyMap(),
                metadataOptions,
                Optional.empty(),
                true
        ).values.stream()
    }

    /**
     * Retrieves the authorized, property data mapped by entity key ids as the origins of the data for each entity set
     * for the given linking ids.
     *
     * @param linkingIdsByEntitySetId map of linked(normal) entity set ids and their linking ids
     * @param authorizedPropertyTypesByEntitySetId map of authorized property types
     * @param extraMetadataOptions set of [MetadataOption]s to include in result (besides the origin id)
     */
    @Timed
    override fun getLinkedEntityDataByLinkingIdWithMetadata(
            linkingIdsByEntitySetId: Map<UUID, Optional<Set<UUID>>>,
            authorizedPropertyTypesByEntitySetId: Map<UUID, Map<UUID, PropertyType>>,
            extraMetadataOptions: EnumSet<MetadataOption>
    ): Map<UUID, Map<UUID, Map<UUID, Map<UUID, Set<Any>>>>> {
        // pair<linking_id to pair<entity_set_id to pair<origin_id to property_data>>>
        val linkedEntityDataStream = dataQueryService.getLinkedEntitiesByEntitySetIdWithOriginIds(
                linkingIdsByEntitySetId,
                authorizedPropertyTypesByEntitySetId,
                extraMetadataOptions
        )

        // linking_id/entity_set_id/origin_id/property_type_id
        val linkedDataMap = HashMap<UUID, MutableMap<UUID, Map<UUID, MutableMap<UUID, MutableSet<Any>>>>>()
        linkedEntityDataStream.forEach {
            val linkingId = it.first
            val entitySetId = it.second.first
            val entityDataById = it.second.second

            if (linkedDataMap.containsKey(linkingId)) {
                linkedDataMap.getValue(linkingId)[entitySetId] = entityDataById
            } else {
                linkedDataMap[linkingId] = mutableMapOf(entitySetId to entityDataById)
            }
        }

        return linkedDataMap
    }

    @Timed
    override fun getLinkedEntitySetBreakDown(
            linkingIdsByEntitySetId: Map<UUID, Optional<Set<UUID>>>,
            authorizedPropertyTypesByEntitySetId: Map<UUID, Map<UUID, PropertyType>>
    )
            : Map<UUID, Map<UUID, Map<UUID, Map<FullQualifiedName, Set<Any>>>>> {
        // pair<linking_id to pair<entity_set_id to pair<origin_id to property_data>>>
        val linkedEntityDataStream = dataQueryService.getLinkedEntitySetBreakDown(
                linkingIdsByEntitySetId,
                authorizedPropertyTypesByEntitySetId
        )

        // linking_id/entity_set_id/origin_id/property_type_id
        val linkedDataMap = HashMap<UUID, MutableMap<UUID, Map<UUID, MutableMap<FullQualifiedName, MutableSet<Any>>>>>()
        linkedEntityDataStream.forEach {
            val linkingId = it.first
            val entitySetId = it.second.first
            val entityDataById = it.second.second

            if (linkedDataMap.containsKey(linkingId)) {
                linkedDataMap.getValue(linkingId)[entitySetId] = entityDataById
            } else {
                linkedDataMap[linkingId] = mutableMapOf(entitySetId to entityDataById)
            }
        }

        return linkedDataMap
    }

    override fun getHistoricalEntitiesById(
            entitySetIds: Map<UUID, Optional<Set<UUID>>>, authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
            metadataOptions: EnumSet<MetadataOption>
    ): Map<UUID, MutableMap<UUID, MutableSet<Property>>> {
        TODO("Not yet implemented")
    }

    override fun getEntitiesAcrossEntitySets(
            entityKeyIds: Map<UUID, Optional<Set<UUID>>>,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
            metadataOptions: EnumSet<MetadataOption>,
            linking: Boolean
    ): Iterable<Pair<UUID, MutableMap<FullQualifiedName, MutableSet<Any>>>> {
        val context = if (linking) {
            linkedEntitiesTimer.time()
        } else {
            entitiesTimer.time()
        }

        try {
            //If the query generated exceed 33.5M UUIDs good chance that it exceed Postgres's 1 GB max query buffer size
            return dataQueryService.getEntitiesWithPropertyTypeFqnsIterable(
                    entityKeyIds,
                    authorizedPropertyTypes,
                    emptyMap(),
                    metadataOptions,
                    Optional.empty(),
                    linking
            )

        } finally {
            context.stop()
        }
    }

    @Timed
    override fun getEntityKeyIdsOfLinkingIds(
            linkingIds: Set<UUID>,
            normalEntitySetIds: Set<UUID>
    ): BasePostgresIterable<Pair<UUID, Set<UUID>>> {
        return linkingQueryService.getEntityKeyIdsOfLinkingIds(linkingIds, normalEntitySetIds)
    }

    /**
     * Delete data of an entity set across ALL sync Ids.
     */
    @SuppressFBWarnings(
            value = ["UC_USELESS_OBJECT"],
            justification = "results Object is used to execute deletes in batches"
    )
    override fun deleteEntitySetData(
            entitySetId: UUID, authorizedPropertyTypes: Map<UUID, PropertyType>
    ): WriteEvent {
        logger.info("Deleting data of entity set: {}", entitySetId)

        val (_, numUpdates) = dataQueryService.deleteEntitySetData(entitySetId, authorizedPropertyTypes)
        val writeEvent = dataQueryService.tombstoneDeletedEntitySet(entitySetId)

        signalEntitySetDataDeleted(entitySetId, DeleteType.Hard)

        // delete entities from linking feedbacks
        val deleteFeedbackCount = feedbackQueryService.deleteLinkingFeedback(entitySetId, Optional.empty())

        // Delete all neighboring entries from matched entities
        val deleteMatchCount = linkingQueryService.deleteEntitySetNeighborhood(entitySetId)

        logger.info(
                "Finished deleting data from entity set {}. " + "Deleted {} rows and {} property data, {} linking feedback and {} matched entries.",
                entitySetId,
                writeEvent.numUpdates,
                numUpdates,
                deleteFeedbackCount,
                deleteMatchCount
        )

        return writeEvent
    }

    override fun deleteEntities(
            entitySetId: UUID,
            entityKeyIds: Set<UUID>,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): WriteEvent {

        val (_, numUpdates) = dataQueryService
                .deleteEntityDataAndEntities(entitySetId, entityKeyIds, authorizedPropertyTypes)
        val writeEvent = dataQueryService.tombstoneDeletedEntities(entitySetId, entityKeyIds)
        signalDeletedEntities(entitySetId, entityKeyIds, DeleteType.Hard)

        // delete entities from linking feedbacks too
        val deleteFeedbackCount = feedbackQueryService.deleteLinkingFeedback(entitySetId, Optional.of(entityKeyIds))

        // Delete all neighboring entries from matched entities
        val deleteMatchCount = linkingQueryService.deleteNeighborhoods(entitySetId, entityKeyIds)

        logger.info(
                "Finished deletion of entities ( {} ) from entity set {}. Deleted {} rows, {} property data, " + "{} linking feedback and {} matched entries.",
                entityKeyIds,
                entitySetId,
                writeEvent.numUpdates,
                numUpdates,
                deleteFeedbackCount,
                deleteMatchCount
        )

        return writeEvent
        TODO("DREW add linking logs")
    }

    override fun deleteEntityProperties(
            entitySetId: UUID,
            entityKeyIds: Set<UUID>,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): WriteEvent {
        val propertyWriteEvent = dataQueryService
                .deleteEntityData(entitySetId, entityKeyIds, authorizedPropertyTypes)

        // same as if we updated the entities
        signalCreatedEntities(entitySetId, entityKeyIds)

        logger.info(
                "Finished deletion of properties ( {} ) from entity set {} and ( {} ) entities. Deleted {} rows " + "of property data",
                authorizedPropertyTypes.values.map(PropertyType::getType),
                entitySetId, entityKeyIds, propertyWriteEvent.numUpdates
        )

        return propertyWriteEvent
    }

    override fun writeEntitiesWithHistory(entities: Map<UUID, MutableMap<UUID, MutableSet<Property>>>) {
        TODO("Not yet implemented")
    }

    override fun getExpiringEntitiesFromEntitySet(
            entitySetId: UUID,
            expirationBaseColumn: String,
            formattedDateMinusTTE: Any,
            sqlFormat: Int,
            deleteType: DeleteType
    ): BasePostgresIterable<UUID> {
        return dataQueryService.getExpiringEntitiesFromEntitySet(
                entitySetId, expirationBaseColumn, formattedDateMinusTTE, sqlFormat, deleteType
        )
    }

}