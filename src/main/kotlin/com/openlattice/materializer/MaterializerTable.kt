package com.openlattice.materializer

import com.openlattice.postgres.PostgresColumn
import com.openlattice.postgres.PostgresDataTables
import com.openlattice.postgres.PostgresTableDefinition

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class MaterializerTable {
    companion object {
        @JvmField
        val DELTAS = PostgresTableDefinition("deltas")
                .addColumns(*PostgresDataTables.dataTableColumns.toTypedArray())
                .primaryKey(PostgresColumn.ENTITY_SET_ID,PostgresColumn.ID_VALUE,PostgresColumn.ORGANIZATION_ID)

    }
}