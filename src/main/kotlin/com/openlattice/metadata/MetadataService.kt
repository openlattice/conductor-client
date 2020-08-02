package com.openlattice.metadata

import com.openlattice.data.DeleteType
import com.openlattice.data.storage.PostgresEntityDataQueryService
import com.openlattice.postgres.streams.BasePostgresIterable
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class MetadataService(private val dataQueryService: PostgresEntityDataQueryService) : MetadataManager {
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