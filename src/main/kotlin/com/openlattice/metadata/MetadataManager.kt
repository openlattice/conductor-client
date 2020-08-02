package com.openlattice.metadata

import com.openlattice.data.DeleteType
import com.openlattice.postgres.streams.BasePostgresIterable
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
interface MetadataManager {
    fun getExpiringEntitiesFromEntitySet(
            entitySetId: UUID,
            expirationBaseColumn: String,
            formattedDateMinusTTE: Any,
            sqlFormat: Int,
            deleteType: DeleteType
    ) : BasePostgresIterable<UUID>

    /**
     * Retrieves the entity key ids for a given set of (linked) entity set and linking ids.
     *
     * @param linkingIdsByEntitySetId Entity set ids to linking ids.
     *
     * @return Entity set id to linking id to entity key id
     */
    fun getLinkedEntityDataKeys(
            linkingIdsByEntitySetId: Map<UUID, Optional<Set<UUID>>>
    ): Map<UUID, Map<UUID, UUID>>
}