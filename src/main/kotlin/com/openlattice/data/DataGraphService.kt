/*
 * Copyright (C) 2018. OpenLattice, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@openlattice.com
 *
 *
 */

package com.openlattice.data

import com.codahale.metrics.annotation.Timed
import com.geekbeast.rhizome.jobs.HazelcastJobService
import com.google.common.base.Stopwatch
import com.google.common.collect.Iterables
import com.google.common.collect.ListMultimap
import com.google.common.collect.Multimaps
import com.google.common.collect.SetMultimap
import com.openlattice.analysis.AuthorizedFilteredNeighborsRanking
import com.openlattice.analysis.requests.AggregationResult
import com.openlattice.analysis.requests.FilteredNeighborsRankingAggregation
import com.openlattice.data.storage.MetadataOption
import com.openlattice.data.storage.StorageManagementService
import com.openlattice.data.storage.StorageMigrationService
import com.openlattice.edm.set.ExpirationBase
import com.openlattice.edm.type.PropertyType
import com.openlattice.graph.core.GraphService
import com.openlattice.graph.core.NeighborSets
import com.openlattice.graph.edge.Edge
import com.openlattice.graph.partioning.RepartitioningJob
import com.openlattice.metadata.MetadataManager
import com.openlattice.postgres.DataTables
import com.openlattice.postgres.PostgresColumn
import com.openlattice.postgres.PostgresDataTables
import com.openlattice.postgres.streams.BasePostgresIterable
import com.openlattice.postgres.streams.PostgresIterable
import org.apache.commons.lang3.tuple.Pair
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.nio.ByteBuffer
import java.sql.Types
import java.time.OffsetDateTime
import java.time.ZoneId
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.stream.Stream
import kotlin.streams.asStream

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
private val logger = LoggerFactory.getLogger(DataGraphService::class.java)

@Service
class DataGraphService(
        private val graphService: GraphService,
        private val idService: EntityKeyIdService,
        private val storageManagement: StorageManagementService,
        private val storageMigration: StorageMigrationService,
        private val metadataManager: MetadataManager,
        private val jobService: HazelcastJobService
) : DataGraphManager {

    companion object {
        const val ASSOCIATION_SIZE = 30_000
    }

    override fun getEntitiesWithMetadata(
            entitySetId: UUID,
            ids: Set<UUID>,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
            metadataOptions: EnumSet<MetadataOption>
    ): Stream<Map<FullQualifiedName, MutableSet<Any>>> =
            getEntitiesWithPropertyTypeFqns(
                    mapOf(entitySetId to Optional.of(ids)),
                    authorizedPropertyTypes,
                    metadataOptions
            ).asSequence().map { entity ->
                entity.mapValues { propertyValues ->
                    propertyValues.value.mapTo(mutableSetOf()) { it.value }
                }
            }.asStream()

    override fun getLinkingEntitiesWithMetadata(
            linkingIdsByEntitySetIds: Map<UUID, Optional<Set<UUID>>>,
            authorizedPropertiesOfNormalEntitySets: Map<UUID, Map<UUID, PropertyType>>,
            metadataOptions: Set<MetadataOption>
    ): Stream<MutableMap<FullQualifiedName, MutableSet<Any>>> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getEntityKeyIdsOfLinkingIds(
            linkingIds: Set<UUID>,
            normalEntitySetIds: Set<UUID>
    ): BasePostgresIterable<kotlin.Pair<UUID, Set<UUID>>> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getEntitiesAcrossEntitySets(
            entitySetIdsToEntityKeyIds: Map<UUID, Set<UUID>>,
            authorizedPropertyTypesByEntitySet: Map<UUID, Map<UUID, PropertyType>>
    ): Map<UUID, Map<UUID, MutableMap<FullQualifiedName, MutableSet<Property>>>> {
        //Since we are no longer assuming a single storage entity here, we have to load a single entity set at a time
        //for now. Long term, we will want to group calls to read all entity sets to each storage provider.
        return entitySetIdsToEntityKeyIds.mapValues { (entitySetId, ids) ->
            val loadingMap = mapOf(entitySetId to Optional.of(ids))
            storageMigration.migrateIfNeeded(mapOf(entitySetId to Optional.of(ids)))
            storageManagement.getReader(entitySetId).getEntities(
                    loadingMap,
                    authorizedPropertyTypesByEntitySet
            ).map { it.first.entityKeyId to it.second }.toMap()
        }
    }

    override fun getEntitiesWithMetadata(
            entityKeyIds: Map<UUID, Optional<Set<UUID>>>,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
            metadataOptions: EnumSet<MetadataOption>
    ): Iterable<MutableMap<FullQualifiedName, MutableSet<Any>>> {
        storageMigration.migrateIfNeeded(entityKeyIds)
        return storageManagement
                .getReaders(entityKeyIds.keys)
                .asSequence()
                .flatMap { (provider, entitySetIds) ->
                    provider.entityLoader.getEntitiesAcrossEntitySets(
                            entitySetIds.associateWith { entityKeyIds.getValue(it) },
                            authorizedPropertyTypes,
                            metadataOptions
                    ).asSequence()
                }.map { it.second }
                .asIterable()
    }

    override fun getEntityKeyIds(entityKeys: Set<EntityKey>): Set<UUID> {
        return idService.reserveEntityKeyIds(entityKeys)
    }

    /* Select */

    override fun getEntitySetData(
            entityKeyIds: Map<UUID, Optional<Set<UUID>>>,
            orderedPropertyNames: LinkedHashSet<String>,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
            linking: Boolean
    ): EntitySetData<FullQualifiedName> {
        storageMigration.migrateIfNeeded(entityKeyIds)
        return EntitySetData<FullQualifiedName>(
                orderedPropertyNames,
                Iterables.concat(
                        *storageManagement
                                .getReaders(entityKeyIds.keys)
                                .entries
                                .map { (provider, entitySetIds) ->
                                    provider.entityLoader.getEntitySetsData(
                                            entitySetIds.associateWith { entityKeyIds.getValue(it) },
                                            orderedPropertyNames,
                                            authorizedPropertyTypes,
                                            linking = linking
                                    ).entities
                                }.toTypedArray()
                )
        )
    }

    override fun getEntity(
            entitySetId: UUID,
            entityKeyId: UUID,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): Map<FullQualifiedName, Set<Any>> = getEntityWithPropertyTypeFqns(
            entitySetId,
            entityKeyId,
            authorizedPropertyTypes
    )
            .mapValues { propertyValues -> propertyValues.value.mapTo(mutableSetOf()) { it.value } }
//    {
//        return eds
//                .getEntitiesWithMetadata(entitySetId, setOf(entityKeyId), mapOf(entitySetId to authorizedPropertyTypes))
//                .iterator().next()
//    }

    override fun getEntityWithPropertyTypeFqns(
            entitySetId: UUID,
            entityKeyId: UUID,
            authorizedPropertyTypes: Map<UUID, PropertyType>,
            metadataOptions: EnumSet<MetadataOption>
    ): MutableMap<FullQualifiedName, MutableSet<Property>> = getEntitiesWithPropertyTypeFqns(
            mapOf(entitySetId to Optional.of(setOf(entityKeyId))),
            mapOf(entitySetId to authorizedPropertyTypes),
            metadataOptions
    ).first()

    override fun getEntityWithPropertyTypeIds(
            entitySetId: UUID, entityKeyId: UUID, authorizedPropertyTypes: Map<UUID, PropertyType>,
            metadataOptions: EnumSet<MetadataOption>
    ): MutableMap<FullQualifiedName, MutableSet<Property>> {
        TODO("Not yet implemented")
    }

    override fun getLinkingEntity(
            entitySetIds: Set<UUID>,
            linkingId: UUID,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>
    ): Map<FullQualifiedName, Set<Any>> {
        val k = Optional.of(setOf(linkingId))
        storageMigration.migrateIfNeeded(
                metadataManager.getLinkedEntityDataKeys(entitySetIds.associateWith { k })
        )

        return storageManagement.getReaders(entitySetIds).entries.fold(mutableMapOf<FullQualifiedName, MutableSet<Any>>()) { linkedEntity, (storageProvider, entitySetIds) ->
            val partialLinkedEntity = storageProvider.entityLoader.getLinkingEntities(
                    entitySetIds.map { it to k }.toMap(),
                    authorizedPropertyTypes
            ).iterator().next()

            partialLinkedEntity.forEach { (propertyFqn, values) ->
                linkedEntity.merge(propertyFqn, values) { oldValues, newValues ->
                    oldValues.addAll(newValues)
                    oldValues
                }
            }
            return@fold linkedEntity
        }
    }

    /**
     * Reads that attempt load an entire entity set will fail until that entity set is migrated.
     *
     * This function is a guard to ensure that we do not try to migrate an entire entity set at once due to a read. It
     * also protects from attempting to load all entity key ids and linking ids in an entity set via [metadataManager#getLinkedEntityDataKeys]
     *
     */
    private fun requireIdsForEntitySetIfMigrating(idsByEntitySetId : Map<UUID, Optional<Set<UUID>>>) {
        //Doing an inline migration of an entire linked entity set
        require(idsByEntitySetId.all { (entitySetId, maybeLinkingIds) ->
            storageMigration.isMigrating(entitySetId) == false || maybeLinkingIds.map { it.isNotEmpty() }.orElse(false)
        }) { "Cannot get linked entity set breakdown for entire entity set, while it is migrating." }
    }

    override fun getLinkedEntitySetBreakDown(
            linkingIdsByEntitySetId: Map<UUID, Optional<Set<UUID>>>,
            authorizedPropertyTypesByEntitySetId: Map<UUID, Map<UUID, PropertyType>>
    ): Map<UUID, Map<UUID, Map<UUID, Map<FullQualifiedName, Set<Any>>>>> {
        requireIdsForEntitySetIfMigrating(linkingIdsByEntitySetId)

        //Migration is slightly more complicated for linked entities, because we need to look up all linked entities
        //and make sure they are migrated.
        storageMigration.migrateIfNeeded(metadataManager.getLinkedEntityDataKeys(linkingIdsByEntitySetId))

        /**
         * In order to allow individual entity datastores to maintain optimizations for reading linked data sets, we
         * group entity sets by storage providers before retrieving and merging the entity set breakdowns.
         */
        val readers = storageManagement.getReaders(linkingIdsByEntitySetId.keys)

        /**
         * Now the magic happens where we fold all the entity breakdowns together.
         */

        return readers.entries.fold(mutableMapOf<UUID, MutableMap<UUID, MutableMap<UUID, MutableMap<FullQualifiedName, MutableSet<Any>>>>>()) { linkedEntityBreakdown, (storageProvider, entitySetIds) ->
            val partialLinkedEntityBreakdown = storageProvider.entityLoader.getLinkedEntitySetBreakDown(
                    entitySetIds.associateWith { linkingIdsByEntitySetId.getValue(it) },
                    authorizedPropertyTypesByEntitySetId
            )

            partialLinkedEntityBreakdown.forEach { (linkingId, breakdown) ->
                linkedEntityBreakdown.merge(linkingId, breakdown) { oldBreakdown, newBreakdown ->
                    newBreakdown.forEach { (entitySetId, entities) ->
                        oldBreakdown.merge(entitySetId, entities) { oldEntities, newEntities ->
                            newEntities.forEach { (id, properties) ->
                                oldEntities.merge(id, properties) { oldProperties, newProperties ->
                                    newProperties.forEach { (propertyFqn, values) ->
                                        oldProperties.merge(propertyFqn, values) { oldValues, newValues ->
                                            oldValues.addAll(newValues)
                                            oldValues
                                        }
                                    }
                                    oldProperties
                                }
                            }
                            oldEntities
                        }
                    }
                    oldBreakdown
                }
            }

            return@fold linkedEntityBreakdown
        }
    }

    override fun getEntitiesWithPropertyTypeFqns(
            entityKeyIds: Map<UUID, Optional<Set<UUID>>>, authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
            metadataOptions: EnumSet<MetadataOption>
    ): Iterable<MutableMap<FullQualifiedName, MutableSet<Property>>> {
        TODO("Not yet implemented")
    }

    override fun getEntitiesWithPropertyTypeIds(
            entityKeyIds: Map<UUID, Optional<Set<UUID>>>, authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
            metadataOptions: EnumSet<MetadataOption>
    ): Iterable<MutableMap<UUID, MutableSet<Property>>> {
        TODO("Not yet implemented")
    }

    override fun getNeighborEntitySets(entitySetIds: Set<UUID>): List<NeighborSets> {
        return graphService.getNeighborEntitySets(entitySetIds)
    }

    override fun getNeighborEntitySetIds(entitySetIds: Set<UUID>): Set<UUID> {
        return getNeighborEntitySets(entitySetIds)
                .flatMap { listOf(it.srcEntitySetId, it.edgeEntitySetId, it.dstEntitySetId) }
                .toSet()
    }

    override fun getEdgesAndNeighborsForVertex(entitySetId: UUID, entityKeyId: UUID): Stream<Edge> {
        return graphService.getEdgesAndNeighborsForVertex(entitySetId, entityKeyId)
    }

    override fun getEdgeKeysOfEntitySet(
            entitySetId: UUID, includeClearedEdges: Boolean
    ): PostgresIterable<DataEdgeKey> {
        return graphService.getEdgeKeysOfEntitySet(entitySetId, includeClearedEdges)
    }

    override fun getEdgesConnectedToEntities(entitySetId: UUID, entityKeyIds: Set<UUID>, includeClearedEdges: Boolean)
            : PostgresIterable<DataEdgeKey> {
        return graphService.getEdgeKeysContainingEntities(entitySetId, entityKeyIds, includeClearedEdges)
    }

    override fun getEdgeEntitySetsConnectedToEntities(entitySetId: UUID, entityKeyIds: Set<UUID>): Set<UUID> {
        return graphService.getEdgeEntitySetsConnectedToEntities(entitySetId, entityKeyIds)
    }

    override fun getEdgeEntitySetsConnectedToEntitySet(entitySetId: UUID): Set<UUID> {
        return graphService.getEdgeEntitySetsConnectedToEntitySet(entitySetId)
    }

    override fun repartitionEntitySet(
            entitySetId: UUID,
            oldPartitions: Set<Int>,
            newPartitions: Set<Int>
    ): UUID {
        return jobService.submitJob(RepartitioningJob(entitySetId, oldPartitions.toList(), newPartitions))
    }

    /* Delete */

    private val groupEdges: (List<DataEdgeKey>) -> Map<UUID, Set<UUID>> = { edges ->
        edges.map { it.edge }.groupBy { it.entitySetId }.mapValues { it.value.map { it.entityKeyId }.toSet() }
    }

    override fun clearAssociationsBatch(
            entitySetId: UUID,
            associationsEdgeKeys: Iterable<DataEdgeKey>,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>
    ): List<WriteEvent> {
        var associationClearCount = 0
        val writeEvents = ArrayList<WriteEvent>()

        associationsEdgeKeys.asSequence().chunked(ASSOCIATION_SIZE).forEach { dataEdgeKeys ->
            val entityKeyIds = groupEdges(dataEdgeKeys)
            entityKeyIds.entries.forEach {
                val writeEvent = clearEntityDataAndVerticesOfAssociations(
                        dataEdgeKeys, it.key, it.value, authorizedPropertyTypes.getValue(it.key)
                )
                writeEvents.add(writeEvent)
                associationClearCount += writeEvent.numUpdates
            }
        }

        logger.info(
                "Cleared {} associations when deleting entities from entity set {}", associationClearCount,
                entitySetId
        )

        return writeEvents
    }

    private fun clearEntityDataAndVerticesOfAssociations(
            dataEdgeKeys: List<DataEdgeKey>,
            entitySetId: UUID,
            entityKeyIds: Set<UUID>,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): WriteEvent {
        // clear edges
        val verticesCount = graphService.clearEdges(dataEdgeKeys)

        //clear entities
        val entityWriteEvent = storageManagement.getWriter(entitySetId).clearEntities(
                entitySetId,
                entityKeyIds,
                authorizedPropertyTypes
        )

        logger.info("Cleared {} entities and {} vertices.", entityWriteEvent.numUpdates, verticesCount)
        return entityWriteEvent
    }

    override fun deleteAssociationsBatch(
            entitySetId: UUID,
            associationsEdgeKeys: Iterable<DataEdgeKey>,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>
    ): List<WriteEvent> {
        var associationDeleteCount = 0
        val writeEvents = ArrayList<WriteEvent>()

        associationsEdgeKeys.asSequence().chunked(ASSOCIATION_SIZE).forEach { dataEdgeKeys ->
            val entityKeyIds = groupEdges(dataEdgeKeys)
            entityKeyIds.entries.forEach {
                val writeEvent = deleteEntityDataAndVerticesOfAssociations(
                        dataEdgeKeys, it.key, it.value, authorizedPropertyTypes.getValue(it.key)
                )
                writeEvents.add(writeEvent)
                associationDeleteCount += writeEvent.numUpdates
            }
        }

        logger.info(
                "Deleted {} associations when deleting entities from entity set {}", associationDeleteCount,
                entitySetId
        )

        return writeEvents
    }

    private fun deleteEntityDataAndVerticesOfAssociations(
            dataEdgeKeys: List<DataEdgeKey>,
            entitySetId: UUID,
            entityKeyIds: Set<UUID>,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): WriteEvent {

        // delete edges
        val verticesCount = graphService.deleteEdges(dataEdgeKeys)

        // delete entities
        val entityWriteEvent = storageManagement
                .getWriter(entitySetId)
                .deleteEntities(entitySetId, entityKeyIds, authorizedPropertyTypes)

        logger.info("Deleted {} entities and {} vertices.", entityWriteEvent.numUpdates, verticesCount.numUpdates)

        return entityWriteEvent
    }

    /* Create */
    override fun createEntities(
            entitySetId: UUID,
            entities: List<MutableMap<UUID, MutableSet<Any>>>,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): Pair<List<UUID>, WriteEvent> {
        //Doesn't require migration since these are all new entities.
        val ids = idService.reserveIds(entitySetId, entities.size)
        val entityMap = ids.mapIndexed { i, id -> id to entities[i] }.toMap()
        val writeEvent = storageManagement
                .getWriter(entitySetId)
                .createOrUpdateEntities(entitySetId, entityMap, authorizedPropertyTypes)

        return Pair.of(ids, writeEvent)
    }

    override fun mergeEntities(
            entitySetId: UUID,
            entities: Map<UUID, MutableMap<UUID, MutableSet<Any>>>,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): WriteEvent {
        storageMigration.migrateIfNeeded(mapOf(entitySetId to Optional.of(entities.keys)))
        return storageManagement.getWriter(entitySetId).createOrUpdateEntities(
                entitySetId,
                entities,
                authorizedPropertyTypes
        )
    }


    override fun replaceEntities(
            entitySetId: UUID,
            entities: Map<UUID, MutableMap<UUID, MutableSet<Any>>>,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): WriteEvent {
        storageMigration.migrateIfNeeded(mapOf(entitySetId to Optional.of(entities.keys)))
        return storageManagement.getWriter(entitySetId).replaceEntities(
                entitySetId,
                entities,
                authorizedPropertyTypes
        )
    }


    override fun partialReplaceEntities(
            entitySetId: UUID,
            entities: Map<UUID, MutableMap<UUID, MutableSet<Any>>>,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): WriteEvent {
        storageMigration.migrateIfNeeded(mapOf(entitySetId to Optional.of(entities.keys)))
        return storageManagement
                .getWriter(entitySetId)
                .partialReplaceEntities(
                        entitySetId,
                        entities,
                        authorizedPropertyTypes
                )
    }

    override fun replacePropertiesInEntities(
            entitySetId: UUID,
            replacementProperties: Map<UUID, Map<UUID, Set<Map<ByteBuffer, Any>>>>,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): WriteEvent {
        storageMigration.migrateIfNeeded(mapOf(entitySetId to Optional.of(replacementProperties.keys)))
        return storageManagement
                .getWriter(entitySetId)
                .replacePropertiesInEntities(entitySetId, replacementProperties, authorizedPropertyTypes)
    }

    override fun createAssociations(associations: Set<DataEdgeKey>): WriteEvent {
        return graphService.createEdges(associations)
    }

    override fun createAssociations(
            associations: ListMultimap<UUID, DataEdge>,
            authorizedPropertiesByEntitySetId: Map<UUID, Map<UUID, PropertyType>>
    ): Map<UUID, CreateAssociationEvent> {

        val associationCreateEvents: MutableMap<UUID, CreateAssociationEvent> = mutableMapOf()

        Multimaps
                .asMap(associations)
                .forEach {
                    val entitySetId = it.key

                    val entities = it.value.map(DataEdge::getData)
                    val (ids, entityWrite) = createEntities(
                            entitySetId, entities, authorizedPropertiesByEntitySetId.getValue(entitySetId)
                    )

                    val edgeKeys = it.value.asSequence().mapIndexed { index, dataEdge ->
                        DataEdgeKey(dataEdge.src, dataEdge.dst, EntityDataKey(entitySetId, ids[index]))
                    }.toSet()
                    val sw = Stopwatch.createStarted()
                    val edgeWrite = graphService.createEdges(edgeKeys)
                    logger.info(
                            "graphService.createEdges (for {} edgeKeys) took {}", edgeKeys.size,
                            sw.elapsed(TimeUnit.MILLISECONDS)
                    )

                    associationCreateEvents[entitySetId] = CreateAssociationEvent(ids, entityWrite, edgeWrite)
                }

        return associationCreateEvents
    }

    /* Top utilizers */
    @Timed
    override fun getFilteredRankings(
            entitySetIds: Set<UUID>,
            numResults: Int,
            filteredRankings: List<AuthorizedFilteredNeighborsRanking>,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
            linked: Boolean,
            linkingEntitySetId: Optional<UUID>
    ): AggregationResult {
        return graphService.computeTopEntities(
                numResults,
                entitySetIds,
                authorizedPropertyTypes,
                filteredRankings,
                linked,
                linkingEntitySetId
        )

    }

    override fun getTopUtilizers(
            entitySetId: UUID,
            filteredNeighborsRankingList: List<FilteredNeighborsRankingAggregation>,
            numResults: Int,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): Stream<SetMultimap<FullQualifiedName, Any>> {
        return Stream.empty()
    }

    override fun getExpiringEntitiesFromEntitySet(
            entitySetId: UUID,
            expirationPolicy: DataExpiration,
            dateTime: OffsetDateTime,
            deleteType: DeleteType,
            expirationPropertyType: Optional<PropertyType>
    ): BasePostgresIterable<UUID> {
        val sqlParams = getSqlParameters(expirationPolicy, dateTime, expirationPropertyType)
        val expirationBaseColumn = sqlParams.first
        val formattedDateMinusTTE = sqlParams.second
        val sqlFormat = sqlParams.third

        return metadataManager.getExpiringEntitiesFromEntitySet(
                entitySetId,
                expirationBaseColumn,
                formattedDateMinusTTE,
                sqlFormat,
                deleteType
        )
    }

    private fun getSqlParameters(
            expirationPolicy: DataExpiration,
            dateTime: OffsetDateTime,
            expirationPT: Optional<PropertyType>
    ): Triple<String, Any, Int> {
        val expirationBaseColumn: String
        val formattedDateMinusTTE: Any
        val sqlFormat: Int
        val dateMinusTTEAsInstant = dateTime.toInstant().minusMillis(expirationPolicy.timeToExpiration)
        when (expirationPolicy.expirationBase) {
            ExpirationBase.DATE_PROPERTY -> {
                val expirationPropertyType = expirationPT.get()
                val columnData = Pair(
                        expirationPropertyType.postgresIndexType,
                        expirationPropertyType.datatype
                )
                expirationBaseColumn = PostgresDataTables.getColumnDefinition(columnData.first, columnData.second).name
                if (columnData.second == EdmPrimitiveTypeKind.Date) {
                    formattedDateMinusTTE = OffsetDateTime.ofInstant(
                            dateMinusTTEAsInstant, ZoneId.systemDefault()
                    ).toLocalDate()
                    sqlFormat = Types.DATE
                } else { //only other TypeKind for date property type is OffsetDateTime
                    formattedDateMinusTTE = OffsetDateTime.ofInstant(dateMinusTTEAsInstant, ZoneId.systemDefault())
                    sqlFormat = Types.TIMESTAMP_WITH_TIMEZONE
                }
            }
            ExpirationBase.FIRST_WRITE -> {
                expirationBaseColumn = "${PostgresColumn.VERSIONS.name}[array_upper(${PostgresColumn.VERSIONS.name},1)]" //gets the latest version from the versions column
                formattedDateMinusTTE = dateMinusTTEAsInstant.toEpochMilli()
                sqlFormat = Types.BIGINT
            }
            ExpirationBase.LAST_WRITE -> {
                expirationBaseColumn = DataTables.LAST_WRITE.name
                formattedDateMinusTTE = OffsetDateTime.ofInstant(dateMinusTTEAsInstant, ZoneId.systemDefault())
                sqlFormat = Types.TIMESTAMP_WITH_TIMEZONE
            }
        }
        return Triple(expirationBaseColumn, formattedDateMinusTTE, sqlFormat)
    }
}