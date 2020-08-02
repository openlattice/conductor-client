package com.openlattice.data.storage

import com.codahale.metrics.Timer
import com.codahale.metrics.annotation.Timed
import com.openlattice.data.DeleteType
import com.openlattice.data.EntityDataKey
import com.openlattice.data.EntitySetData
import com.openlattice.data.Property
import com.openlattice.edm.type.PropertyType
import com.openlattice.postgres.streams.BasePostgresIterable
import org.apache.olingo.commons.api.edm.FullQualifiedName
import java.nio.ByteBuffer
import java.util.*
import java.util.stream.Stream

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
interface EntityLoader {
    val entitiesTimer: Timer
    val linkedEntitiesTimer: Timer

    /**
     * Loads all of the entities in an entity set.
     *
     * @param entitySetId The entity set id for which to load data
     * @param authorizedPropertyTypes The property types for which to load data.
     *
     * @return All of entities with given property types and metadata
     */
    fun getAllEntitiesWithMetadata(
            entitySetId: UUID,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
            metadataOptions: EnumSet<MetadataOption>
    ): Map<UUID, MutableMap<FullQualifiedName, MutableSet<Any>>>

    /**
     * Loads specific entities and associated metadata.
     *
     * @param entitySetId The entity set id for which to load data
     * @param ids The entity key ids for which to load data and metadata
     * @param authorizedPropertyTypes The property types for which to load data.
     * @param metadataOptions The metadata which to read.
     *
     * @return A map of entities where the keys are ids and the values are the entities.
     */
    fun getEntitiesWithMetadata(
            entitySetId: UUID,
            ids: Set<UUID>,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
            metadataOptions: EnumSet<MetadataOption> = EnumSet.noneOf(MetadataOption::class.java)
    ): Map<UUID, MutableMap<FullQualifiedName, MutableSet<Any>>>

    fun getLinkingEntities(
            entityKeyIds: Map<UUID, Optional<Set<UUID>>>,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>
    ): Stream<MutableMap<FullQualifiedName, MutableSet<Any>>>

    fun getLinkingEntitiesWithMetadata(
            entityKeyIds: Map<UUID, Optional<Set<UUID>>>,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
            metadataOptions: EnumSet<MetadataOption>
    ): Stream<MutableMap<FullQualifiedName, MutableSet<Any>>>

    fun getLinkedEntityDataByLinkingIdWithMetadata(
            linkingIdsByEntitySetId: Map<UUID, Optional<Set<UUID>>>,
            authorizedPropertyTypesByEntitySetId: Map<UUID, Map<UUID, PropertyType>>,
            extraMetadataOptions: EnumSet<MetadataOption>
    ): Map<UUID, Map<UUID, Map<UUID, Map<UUID, Set<Any>>>>>

    fun getLinkedEntitySetBreakDown(
            linkingIdsByEntitySetId: Map<UUID, Optional<Set<UUID>>>,
            authorizedPropertyTypesByEntitySetId: Map<UUID, Map<UUID, PropertyType>>
    ): MutableMap<UUID, MutableMap<UUID, MutableMap<UUID, MutableMap<FullQualifiedName, MutableSet<Any>>>>>

    /**
     * Returns data from either at least one regular entity sets or at least one linked entity set.
     *
     * It is designed so that underlying implementation can return a stream of data.
     *
     * @param entityKeyIds Map of entity set ids to an optioanl set of entity key ids to load.
     *
     * @return Returns an iterable of pairs consisting of the entity data key and the entity.
     */
    fun getEntities(
            entityKeyIds: Map<UUID, Optional<Set<UUID>>>,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
            metadataOptions: EnumSet<MetadataOption> = EnumSet.noneOf(MetadataOption::class.java),
            linked: Boolean = false,
            detailed: Boolean = false
    ) : Iterable<Pair<EntityDataKey, MutableMap<FullQualifiedName, MutableSet<Property>>>>


    /**
     * Loads specific entities and associated metadata.
     *
     * @param entityKeyIds The set of ids to load for each entity set
     * @param authorizedPropertyTypes The property types to load for each data set.
     * @param metadataOptions The metadata to read.
     *
     * @return A iterable of entities with corresponding property types and metadata.
     */
    fun getEntitiesAcrossEntitySetsWithFqns(
            entityKeyIds: Map<UUID, Optional<Set<UUID>>>,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
            metadataOptions: EnumSet<MetadataOption> = EnumSet.noneOf(MetadataOption::class.java)
    ): Iterable<Pair<EntityDataKey, MutableMap<FullQualifiedName, MutableSet<Property>>>>

    /**
     * Loads specific entities and associated metadata.
     *
     * @param entityKeyIds The set of ids to load for each entity set
     * @param authorizedPropertyTypes The property types to load for each entity set.
     * @param metadataOptions The metadata to read.
     *
     * @return A iterable of entities with corresponding property types and metadata.
     */
    fun getEntitiesAcrossEntitySetsWithIds(
            entityKeyIds: Map<UUID, Optional<Set<UUID>>>,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
            metadataOptions: EnumSet<MetadataOption> = EnumSet.noneOf(MetadataOption::class.java)
    ): Iterable<Pair<EntityDataKey, Collection<MutableMap<UUID, MutableSet<Any>>>>>

    /**
     * Loads specific entities and associated metadata.
     *
     * @param entityKeyIds The set of ids to load for each entity set
     * @param authorizedPropertyTypes The property types to load for each data set.
     * @param metadataOptions The metadata to read.
     *
     * @return A iterable of entities with corresponding property types and metadata.
     */
    @Deprecated(message = "Legacy Data API that doesn't return entity set id of entities.")
    fun getEntitiesAcrossEntitySets(
            entityKeyIds: Map<UUID, Optional<Set<UUID>>>,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
            metadataOptions: EnumSet<MetadataOption> = EnumSet.noneOf(MetadataOption::class.java),
            linking: Boolean = false
    ): Iterable<Pair<UUID, MutableMap<FullQualifiedName, MutableSet<Any>>>>

    /**
     *
     */
    fun getHistoricalEntitiesById(
            entitySetIds: Map<UUID, Optional<Set<UUID>>>,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
            metadataOptions: EnumSet<MetadataOption> = EnumSet.allOf(MetadataOption::class.java)
    ): Map<UUID, MutableMap<UUID, MutableSet<Property>>>



    @JvmDefault
    @Timed
    fun getEntitySetsData(
            entityKeyIds: Map<UUID, Optional<Set<UUID>>>,
            orderedPropertyTypes: LinkedHashSet<String>,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
            metadataOptions: EnumSet<MetadataOption> = EnumSet.noneOf(MetadataOption::class.java),
            linking: Boolean = false
    ): EntitySetData<FullQualifiedName> {
        val context = if (linking) {
            linkedEntitiesTimer.time()
        } else {
            entitiesTimer.time()
        }

        //If the query generated exceed 33.5M UUIDs good chance that it exceed Postgres's 1 GB max query buffer size

        val entitySetData = EntitySetData(
                orderedPropertyTypes,
                getEntitiesAcrossEntitySets(
                        entityKeyIds,
                        authorizedPropertyTypes,
                        metadataOptions,
                        linking
                ).map { it.second } //Force query results into memory to avoid holding connections open unnecessarily
        )

        context.stop()

        return entitySetData
    }

    /**
     * Loads data from multiple entity sets. Note: not implemented for linking entity sets!
     *
     * @param entitySetIdsToEntityKeyIds map of entity sets to entity keys for which the data should be loaded
     * @param authorizedPropertyTypesByEntitySet map of entity sets and the property types for which the user is authorized
     * @return map of entity set ids to list of entity data
     */
    @Deprecated(
            "Only available for SearchService compatibility",
            replaceWith = ReplaceWith("getEntitiesAcrossEntitySets")
    )
    @JvmDefault
    fun getEntitiesAcrossEntitySets(
            entitySetIdsToEntityKeyIds: Map<UUID, Set<UUID>>,
            authorizedPropertyTypesByEntitySet: Map<UUID, Map<UUID, PropertyType>>
    ): Map<UUID, Collection<MutableMap<FullQualifiedName, MutableSet<Any>>>> {
        return getEntitiesAcrossEntitySets(
                entitySetIdsToEntityKeyIds.mapValues { Optional.of(it.value) },
                authorizedPropertyTypesByEntitySet,
                linking = false
        ).groupBy({ it.first }, { it.second })
    }

    fun getEntityKeyIdsOfLinkingIds(
            linkingIds: Set<UUID>,
            normalEntitySetIds: Set<UUID>
    ): BasePostgresIterable<Pair<UUID, Set<UUID>>>



}