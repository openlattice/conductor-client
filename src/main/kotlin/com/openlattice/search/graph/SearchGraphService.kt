/*
 * Copyright (C) 2019. OpenLattice, Inc.
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
package com.openlattice.search.graph

import com.codahale.metrics.annotation.Timed
import com.google.common.base.Stopwatch
import com.google.common.collect.Maps
import com.openlattice.authorization.*
import com.openlattice.data.requests.NeighborEntityDetails
import com.openlattice.data.requests.NeighborEntityIds
import com.openlattice.data.storage.EntityDatastore
import com.openlattice.datastore.services.EdmManager
import com.openlattice.datastore.services.EntitySetManager
import com.openlattice.edm.EntitySet
import com.openlattice.edm.set.EntitySetFlag
import com.openlattice.edm.type.PropertyType
import com.openlattice.graph.core.GraphService
import com.openlattice.graph.edge.Edge
import com.openlattice.ids.IdCipherManager
import com.openlattice.search.SearchService
import com.openlattice.search.requests.EntityNeighborsFilter
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.stream.Collectors

@Service
class SearchGraphService(
        private val dataManager: EntityDatastore,
        private val graphService: GraphService,
        private val dataModelService: EdmManager,
        private val entitySetService: EntitySetManager,
        private val idCipher: IdCipherManager,
        private val authorizations: AuthorizationManager
) {

    companion object {
        private val logger = LoggerFactory.getLogger(SearchGraphService::class.java)
    }

    @Timed
    fun executeEntityNeighborSearch(
            filter: EntityNeighborsFilter,
            principals: Set<Principal>
    ): Map<UUID, List<NeighborEntityDetails>> {
        val sw = Stopwatch.createStarted()

        logger.info("Starting Entity Neighbor Search...")
        if (checkAssociationFilterMissing(filter)) {
            logger.info("Missing association entity set ids.. returning empty result")
            return mapOf()
        }

        // handle first linked entity sets
        val linkingEntitySets = entitySetService
                .getEntitySetsWithFlags(filter.entityKeyIds.keys, EnumSet.of(EntitySetFlag.LINKING))

        val groupedEntityKeyIds = filter.entityKeyIds.entries.groupBy { linkingEntitySets.keys.contains(it.key) }

        var entityNeighbors: MutableMap<UUID, MutableList<NeighborEntityDetails>> = Maps.newConcurrentMap()

        if (linkingEntitySets.isNotEmpty()) {
            entityNeighbors = executeLinkingEntityNeighborSearch(
                    linkingEntitySets,
                    EntityNeighborsFilter(
                            groupedEntityKeyIds.getValue(true).map { it.key to it.value }.toMap(),
                            filter.srcEntitySetIds,
                            filter.dstEntitySetIds,
                            filter.associationEntitySetIds),
                    principals
            )
        }

        val normalEntityKeyIds = groupedEntityKeyIds.getValue(false).map { it.key to it.value }.toMap()
        val entityKeyIds = normalEntityKeyIds.values.flatten().toSet()
        collectEntityNeighborDetails(filter, entityNeighbors, normalEntityKeyIds, entityKeyIds, principals)

        logger.info("Finished entity neighbor search in {} ms", sw.elapsed(TimeUnit.MILLISECONDS))
        return entityNeighbors
    }

    @Timed
    private fun executeLinkingEntityNeighborSearch(
            linkedEntitySets: Map<UUID, EntitySet>,
            filter: EntityNeighborsFilter,
            principals: Set<Principal>
    ): MutableMap<UUID, MutableList<NeighborEntityDetails>> {
        val sw1 = Stopwatch.createStarted()
        val sw2 = Stopwatch.createStarted()

        val linkingEntityNeighbors: MutableMap<UUID, MutableList<NeighborEntityDetails>> = Maps.newConcurrentMap()

        // we need to create separate entries for same linking id but in different linked entity sets
        filter.entityKeyIds.forEach { (linkedEntitySetId, linkingIds) ->
            logger.info("Starting search for linked entity set {}.", linkedEntitySetId)

            val normalEntitySetIds = linkedEntitySets.getValue(linkedEntitySetId).linkedEntitySets
            val entityKeyIdsByLinkingId = getEntityKeyIdsOfLinkingIds(linkingIds, linkedEntitySets.getValue(linkedEntitySetId))

            val entityKeyIds = entityKeyIdsByLinkingId.values.flatten().toSet()
            val normalEntityKeyIds = normalEntitySetIds.map { it to entityKeyIds }.toMap()

            val entityNeighbors = Maps.newConcurrentMap<UUID, MutableList<NeighborEntityDetails>>()
            collectEntityNeighborDetails(filter, entityNeighbors, normalEntityKeyIds, entityKeyIds, principals)


            /* Map linkingIds to the collection of neighbors for all entityKeyIds in the cluster */
            val encryptedLinkingIds = idCipher.encryptIdsAsMap(linkedEntitySetId, linkingIds)
            entityKeyIdsByLinkingId.forEach { (linkingId, normalEntityKeyIds) ->
                linkingEntityNeighbors[encryptedLinkingIds.getValue(linkingId)] = normalEntityKeyIds
                        .flatMap { entityKeyId -> entityNeighbors.getOrDefault(entityKeyId, mutableListOf()) }
                        .toMutableList()
            }

            logger.info(
                    "Finished entity neighbor search for linked entity set {} in {} ms",
                    linkedEntitySetId,
                    sw2.elapsed(TimeUnit.MILLISECONDS)
            )
            sw2.reset().start()
        }

        logger.info("Finished linking entity neighbor search in {} ms", sw1.elapsed(TimeUnit.MILLISECONDS))
        return linkingEntityNeighbors
    }

    private fun collectEntityNeighborDetails(
            filter: EntityNeighborsFilter,
            entityNeighbors: MutableMap<UUID, MutableList<NeighborEntityDetails>>,
            normalEntityKeyIds: Map<UUID, Set<UUID>>,
            entityKeyIds: Set<UUID>,
            principals: Set<Principal>
    ) {
        val sw1 = Stopwatch.createStarted()

        val edges = mutableListOf<Edge>()
        val allEntitySetIds = mutableSetOf<UUID>()

        graphService
                .getEdgesAndNeighborsForVerticesBulk(
                        EntityNeighborsFilter(
                                normalEntityKeyIds,
                                filter.srcEntitySetIds,
                                filter.dstEntitySetIds,
                                filter.associationEntitySetIds))
                .forEach { edge ->
                    edges.add(edge)
                    allEntitySetIds.add(edge.edge.entitySetId)
                    allEntitySetIds.add(
                            if (entityKeyIds.contains(edge.src.entityKeyId)) {
                                edge.dst.entitySetId
                            } else {
                                edge.src.entitySetId
                            }
                    )
                }

        logger.info(
                "Get edges and neighbors for vertices query for {} ids finished in {} ms",
                entityKeyIds.size,
                sw1.elapsed(TimeUnit.MILLISECONDS)
        )
        sw1.reset().start()

        val authorizedEntitySetIds = getAuthorizedEntitySetIds(allEntitySetIds, principals, true)
        val entitySetsById = entitySetService.getEntitySetsAsMap(authorizedEntitySetIds)
        val (entitySetsIdsToAuthorizedProps, authorizedEdgeESIdsToVertexESIds) =
                getAuthorizedPropertyTypesAndNeighborEntitySetIds(entitySetsById, principals)

        logger.info(
                "Access checks for entity sets and their properties finished in {} ms",
                sw1.elapsed(TimeUnit.MILLISECONDS)
        )
        sw1.reset().start()

        val entitySetIdToEntityKeyId = mutableMapOf<UUID, MutableSet<UUID>>()
        edges.forEach { edge ->
            val neighborEntityKeyId = if (entityKeyIds.contains(edge.src.entityKeyId)) {
                edge.dst.entityKeyId
            } else {
                edge.src.entityKeyId
            }

            val neighborEntitySetId = if (entityKeyIds.contains(edge.src.entityKeyId)) {
                edge.dst.entitySetId
            } else {
                edge.src.entitySetId
            }

            val edgeEntitySetId = edge.edge.entitySetId
            val edgeEntityKeyId = edge.edge.entityKeyId

            if (entitySetsIdsToAuthorizedProps.containsKey(edgeEntitySetId)) {
                entitySetIdToEntityKeyId.putIfAbsent(edgeEntitySetId, mutableSetOf())
                entitySetIdToEntityKeyId.getValue(edgeEntitySetId).add(edgeEntityKeyId)

                if (entitySetsIdsToAuthorizedProps.containsKey(neighborEntitySetId)) {
                    authorizedEdgeESIdsToVertexESIds.getValue(edgeEntitySetId).add(neighborEntitySetId)
                    entitySetIdToEntityKeyId.putIfAbsent(neighborEntitySetId, mutableSetOf())
                    entitySetIdToEntityKeyId.getValue(neighborEntitySetId).add(neighborEntityKeyId)
                }
            }

        }

        logger.info("Edge and neighbor entity key ids collected in {} ms", sw1.elapsed(TimeUnit.MILLISECONDS))
        sw1.reset().start()

        val entitiesAcrossEntitySetIds =
                dataManager.getEntitiesAcrossEntitySets(entitySetIdToEntityKeyId, entitySetsIdsToAuthorizedProps)

        logger.info("Get entities across entity sets query finished in {} ms", sw1.elapsed(TimeUnit.MILLISECONDS))
        sw1.reset().start()

        val entities = entitiesAcrossEntitySetIds.map { SearchService.getEntityKeyId(it) to it }.toMap()
        // create a NeighborEntityDetails object for each edge based on authorizations
        edges.parallelStream().forEach { edge ->
            val vertexIsSrc = entityKeyIds.contains(edge.key.src.entityKeyId)
            val entityId = if (vertexIsSrc) {
                edge.key.src.entityKeyId
            } else {
                edge.key.dst.entityKeyId
            }
            entityNeighbors.putIfAbsent(entityId, Collections.synchronizedList(mutableListOf()))

            val neighbor = getNeighborEntityDetails(
                    edge,
                    authorizedEdgeESIdsToVertexESIds,
                    entitySetsById,
                    vertexIsSrc,
                    entities
            )
            if (neighbor != null) {
                entityNeighbors.getValue(entityId).add(neighbor)
            }
        }

        logger.info("Neighbor entity details collected in {} ms", sw1.elapsed(TimeUnit.MILLISECONDS))
        sw1.reset().start()

    }

    private fun getNeighborEntityDetails(
            edge: Edge,
            authorizedEdgeESIdsToVertexESIds: Map<UUID, Set<UUID>>,
            entitySetsById: Map<UUID, EntitySet>,
            vertexIsSrc: Boolean,
            entities: Map<UUID, Map<FullQualifiedName, Set<Any>>>
    ): NeighborEntityDetails? {

        val edgeEntitySetId = edge.edge.entitySetId
        if (authorizedEdgeESIdsToVertexESIds.containsKey(edgeEntitySetId)) {
            val neighborEntityKeyId = if (vertexIsSrc) {
                edge.dst.entityKeyId
            } else {
                edge.src.entityKeyId
            }
            val neighborEntitySetId = if (vertexIsSrc) {
                edge.dst.entitySetId
            } else {
                edge.src.entitySetId
            }

            val edgeDetails = entities[edge.edge.entityKeyId]
            if (edgeDetails != null) {
                if (authorizedEdgeESIdsToVertexESIds.getValue(edgeEntitySetId).contains(neighborEntitySetId)) {
                    val neighborDetails = entities[neighborEntityKeyId]

                    if (neighborDetails != null) {
                        return NeighborEntityDetails(
                                entitySetsById[edgeEntitySetId],
                                edgeDetails,
                                entitySetsById[neighborEntitySetId],
                                neighborEntityKeyId,
                                neighborDetails,
                                vertexIsSrc)
                    }

                } else {
                    return NeighborEntityDetails(
                            entitySetsById[edgeEntitySetId],
                            edgeDetails,
                            vertexIsSrc)
                }
            }
        }

        return null
    }


    @Timed
    fun executeEntityNeighborIdsSearch(
            filter: EntityNeighborsFilter, principals: Set<Principal>
    ): Map<UUID, Map<UUID, Map<UUID, Set<NeighborEntityIds>>>> {
        val sw = Stopwatch.createStarted()

        logger.info("Starting Reduced Entity Neighbor Search...")
        if (checkAssociationFilterMissing(filter)) {
            logger.info("Missing association entity set ids. Returning empty result.")
            return mapOf()
        }

        val entityKeyIds = filter.entityKeyIds.values.flatten().toSet()
        val allEntitySetIds = mutableSetOf<UUID>()
        val neighbors = mutableMapOf<UUID, MutableMap<UUID, MutableMap<UUID, MutableSet<NeighborEntityIds>>>>()

        graphService.getEdgesAndNeighborsForVerticesBulk(filter).forEach { edge ->

            val isSrc = entityKeyIds.contains(edge.src.entityKeyId)
            val entityKeyId = if (isSrc) edge.src.entityKeyId else edge.dst.entityKeyId
            val neighborEntityDataKey = if (isSrc) edge.dst else edge.src

            val neighborEntityIds = NeighborEntityIds(edge.edge.entityKeyId, neighborEntityDataKey.entityKeyId, isSrc)
            val edgeEsId = edge.edge.entitySetId
            val neighborEsId = neighborEntityDataKey.entitySetId

            neighbors.putIfAbsent(entityKeyId, mutableMapOf())
            neighbors.getValue(entityKeyId).putIfAbsent(edgeEsId, mutableMapOf())
            neighbors.getValue(entityKeyId).getValue(edgeEsId).putIfAbsent(neighborEsId, mutableSetOf())
            neighbors.getValue(entityKeyId).getValue(edgeEsId).getValue(neighborEsId).add(neighborEntityIds)

            allEntitySetIds.add(edgeEsId)
            allEntitySetIds.add(neighborEsId)
        }

        val unauthorizedEntitySetIds = getAuthorizedEntitySetIds(allEntitySetIds, principals, false)


        if (unauthorizedEntitySetIds.isNotEmpty()) {
            neighbors.values.forEach { associationMap ->
                associationMap.values.forEach { neighborsMap ->
                    neighborsMap.entries.removeIf { neighborEntry ->
                        unauthorizedEntitySetIds.contains(neighborEntry.key)
                    }
                }
                associationMap.entries.removeIf { entry ->
                    unauthorizedEntitySetIds.contains(entry.key) || entry.value.isEmpty()
                }
            }
        }

        logger.info("Reduced entity neighbor search took {} ms", sw.elapsed(TimeUnit.MILLISECONDS))

        return neighbors
    }

    @Timed
    fun executeLinkingEntityNeighborIdsSearch(
            linkedEntitySets: Map<UUID, EntitySet>,
            filter: EntityNeighborsFilter,
            principals: Set<Principal>
    ): Map<UUID, Map<UUID, Map<UUID, Set<NeighborEntityIds>>>> {
        if (checkAssociationFilterMissing(filter)) {
            return mapOf()
        }

        // If there are multiple linked entity sets for one linking id, they need to be returned as a separate entry
        val linkingEntityNeighbors = mutableMapOf<UUID, MutableMap<UUID, Map<UUID, Set<NeighborEntityIds>>>>()

        filter.entityKeyIds.forEach { (linkedEntitySetId, linkingIds) ->
            val entityKeyIdsByLinkingIds = getEntityKeyIdsOfLinkingIds(linkingIds, linkedEntitySets.getValue(linkedEntitySetId))
            val entityKeyIds = entityKeyIdsByLinkingIds.values.flatten().toSet()

            // Will return only entries, where there is at least 1 neighbor
            val entityNeighbors = executeEntityNeighborIdsSearch(
                    EntityNeighborsFilter(
                            linkedEntitySets.getValue(linkedEntitySetId).linkedEntitySets
                                    .map { it to entityKeyIds }.toMap(),
                            filter.srcEntitySetIds,
                            filter.dstEntitySetIds,
                            filter.associationEntitySetIds),
                    principals
            )

            if (entityNeighbors.isNotEmpty()) {
                val encryptedLinkingIds = idCipher.encryptIdsAsMap(linkedEntitySetId, entityKeyIdsByLinkingIds.keys)
                entityKeyIdsByLinkingIds
                        .filter { (_, entityKeyIds) -> entityKeyIds.any { entityNeighbors.keys.contains(it) } }
                        .forEach { (linkingId, entityKeyIds) ->
                            val neighborIds = Maps.newHashMapWithExpectedSize<UUID, Map<UUID, Set<NeighborEntityIds>>>(
                                    entityKeyIds.size
                            )
                            entityKeyIds
                                    .filter { entityNeighbors.containsKey(it) }
                                    .forEach { entityKeyId ->
                                        entityNeighbors.getValue(entityKeyId).forEach { (key, value) ->
                                            neighborIds[key] = value
                                        }
                                    }

                            linkingEntityNeighbors[encryptedLinkingIds.getValue(linkingId)] = neighborIds
                        }
            }
        }

        return linkingEntityNeighbors
    }

    /**
     * Decrypts and collects entity key ids belonging to the requested encrypted linking ids.
     *
     * @param linkingIds      Encrypted linking ids.
     * @param linkedEntitySet The linked entity sets.
     * @return Map of entity key ids mapped by their linking ids.
     */
    private fun getEntityKeyIdsOfLinkingIds(linkingIds: Set<UUID>, linkedEntitySet: EntitySet): Map<UUID, Set<UUID>> {
        val decryptedLinkingIds = idCipher.decryptIds(linkedEntitySet.id, linkingIds)
        return dataManager.getEntityKeyIdsByLinkingIds(decryptedLinkingIds, linkedEntitySet.linkedEntitySets).toMap()
    }

    private fun checkAssociationFilterMissing(filter: EntityNeighborsFilter): Boolean {
        return filter.associationEntitySetIds.isPresent && filter.associationEntitySetIds.get().isEmpty()
    }

    /* Authorization checks */

    private fun getAuthorizedEntitySetIds(
            ids: Set<UUID>, principals: Set<Principal>, keepAuthorized: Boolean
    ): Set<UUID> {
        return authorizations
                .accessChecksForPrincipals(
                        ids.map { AccessCheck(AclKey(it), EdmAuthorizationHelper.READ_PERMISSION) }.toSet(),
                        principals
                )
                .filter { it.permissions.getValue(Permission.READ) == keepAuthorized } //xnor
                .map { it.aclKey[0] }
                .collect(Collectors.toSet())
    }

    private fun getAuthorizedPropertyTypesAndNeighborEntitySetIds(
            entitySetsById: Map<UUID, EntitySet>,
            principals: Set<Principal>
    ): Pair<Map<UUID, MutableMap<UUID, PropertyType>>, Map<UUID, MutableSet<UUID>>> {
        val entitySetsIdsToAuthorizedProps = Maps.newHashMapWithExpectedSize<UUID, MutableMap<UUID, PropertyType>>(
                entitySetsById.size
        )
        val authorizedEdgeESIdsToVertexESIds = Maps.newHashMapWithExpectedSize<UUID, MutableSet<UUID>>(
                entitySetsById.size
        )

        val entityTypesById = dataModelService.getEntityTypesAsMap(
                entitySetsById.values.map { entitySet ->
                    entitySetsIdsToAuthorizedProps[entitySet.id] = mutableMapOf()
                    authorizedEdgeESIdsToVertexESIds[entitySet.id] = mutableSetOf()
                    entitySet.entityTypeId
                }.toSet()
        )

        val propertyTypesById = dataModelService.getPropertyTypesAsMap(
                entityTypesById.values.flatMap { entityType -> entityType.properties }.toSet()
        )

        val accessChecks = entitySetsById.values
                .flatMap { entitySet ->
                    entityTypesById.getValue(entitySet.entityTypeId).properties
                            .map { propertyTypeId ->
                                AccessCheck(
                                        AclKey(entitySet.id, propertyTypeId),
                                        EdmAuthorizationHelper.READ_PERMISSION
                                )
                            }
                }.toSet()

        authorizations.accessChecksForPrincipals(accessChecks, principals).forEach { auth ->
            if (auth.permissions.getValue(Permission.READ)) {
                val esId = auth.aclKey[0]
                val ptId = auth.aclKey[1]
                entitySetsIdsToAuthorizedProps.getValue(esId)[ptId] = propertyTypesById.getValue(ptId)
            }
        }

        return (entitySetsIdsToAuthorizedProps to authorizedEdgeESIdsToVertexESIds)
    }
}