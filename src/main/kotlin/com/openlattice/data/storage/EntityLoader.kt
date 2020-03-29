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

    fun getEntities(
            entitySetId: UUID,
            ids: Set<UUID>,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>
    ): Stream<MutableMap<FullQualifiedName, MutableSet<Any>>>

    fun getEntitiesWithMetadata(
            entitySetId: UUID,
            ids: Set<UUID>,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
            metadataOptions: EnumSet<MetadataOption>
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

    fun getEntities(
            entityKeyIds: Map<UUID, Optional<Set<UUID>>>,
            orderedPropertyTypes: LinkedHashSet<String>,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
            linking: Boolean
    ): EntitySetData<FullQualifiedName>

    fun getEntitiesAcrossEntitySets(
            entitySetIdsToEntityKeyIds: SetMultimap<UUID, UUID>,
            authorizedPropertyTypesByEntitySet: Map<UUID, Map<UUID, PropertyType>>
    ): Map<UUID, Collection<MutableMap<FullQualifiedName, MutableSet<Any>>>>

    fun getEntityKeyIdsOfLinkingIds(
            linkingIds: Set<UUID>,
            normalEntitySetIds: Set<UUID>
    ): BasePostgresIterable<Pair<UUID, Set<UUID>>>
}