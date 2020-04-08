package com.openlattice.data.storage

import com.google.common.collect.SetMultimap
import com.openlattice.data.EntitySetData
import com.openlattice.edm.type.PropertyType
import com.openlattice.postgres.streams.BasePostgresIterable
import org.apache.olingo.commons.api.edm.FullQualifiedName
import java.util.*
import java.util.stream.Stream

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
interface EntityLoader {

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
    ): Stream<MutableMap<FullQualifiedName, MutableSet<Any>>>

    /**
     * Loads specific entities and associated metadata.
     *
     * @param entitySetId The entity set id for which to load data
     * @param ids The entity key ids for which to load data and metadata
     * @param authorizedPropertyTypes The property types for which to load data.
     * @param metadataOptions The metadata which to read.
     *
     * @return A stream of entities with corresponding property types and metadata.
     */
    fun getEntitiesWithMetadata(
            entitySetId: UUID,
            ids: Set<UUID>,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
            metadataOptions: EnumSet<MetadataOption> = EnumSet.noneOf(MetadataOption::class.java)
    ): Stream<MutableMap<FullQualifiedName, MutableSet<Any>>>

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
    ): Map<UUID, Map<UUID, Map<UUID, Map<FullQualifiedName, Set<Any>>>>>


    /**
     * Loads specific entities and associated metadata.
     *
     * @param entitySetId The entity set id for which to load data
     * @param ids The entity key ids for which to load data and metadata
     * @param authorizedPropertyTypes The property types for which to load data.
     * @param metadataOptions The metadata which to read.
     *
     * @return A stream of entities with corresponding property types and metadata.
     */
    fun getEntitiesAcrossEntitySets(
            entityKeyIds: Map<UUID, Optional<Set<UUID>>>,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
            metadataOptions: EnumSet<MetadataOption> = EnumSet.noneOf(MetadataOption::class.java),
            linking: Boolean = false
    ): Iterable<Pair<UUID,Map<FullQualifiedName,Set<Any>>>>

    fun getEntitySetData(
            entityKeyIds: Map<UUID, Optional<Set<UUID>>>,
            orderedPropertyTypes: LinkedHashSet<String>,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
            metadataOptions: EnumSet<MetadataOption> = EnumSet.noneOf(MetadataOption::class.java),
            linking: Boolean = false
    ): EntitySetData<FullQualifiedName>

    fun getEntitiesAcrossEntitySets(
            entitySetIdsToEntityKeyIds: Map<UUID, Set<UUID>>,
            authorizedPropertyTypesByEntitySet: Map<UUID, Map<UUID, PropertyType>>
    ): Map<UUID, Collection<MutableMap<FullQualifiedName, MutableSet<Any>>>>
//    {
//        return getEntitiesAcrossEntitySets(
//                en
//        )
//    }

    fun getEntityKeyIdsOfLinkingIds(
            linkingIds: Set<UUID>,
            normalEntitySetIds: Set<UUID>
    ): BasePostgresIterable<Pair<UUID, Set<UUID>>>
}