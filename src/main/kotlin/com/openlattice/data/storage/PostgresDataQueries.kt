package com.openlattice.data.storage


import com.openlattice.edm.type.PropertyType
import com.openlattice.postgres.*
import com.openlattice.postgres.DataTables.LAST_WRITE
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresDataTables.Companion.getColumnDefinition
import com.openlattice.postgres.PostgresTable.DATA
import java.util.*

/**
 * This class is responsible for generating all the SQL for creating, reading, upated, and deleting entities.
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
internal class PostgresDataQueries

const val VALUES = "values"
val dataMetadataColumnsParametersSql = PostgresDataTables.dataTableMetadataColumns.joinToString(",") { "?" }
val dataMetadataColumnsSql = PostgresDataTables.dataTableMetadataColumns.joinToString { "," }
val dataTableColumnsSql = PostgresDataTables.dataTableColumns.joinToString(",") { it.name }
val dataTableColumnsBindSql = PostgresDataTables.dataTableColumns.joinToString(",") { "?" }

val dataTableColumnsConflictSetSql = PostgresDataTables.dataTableColumns.joinToString(",") {
    "${it.name} = EXCLUDED.${it.name}"
}

val valuesColumnsSql = PostgresDataTables.dataTableValueColumns.joinToString(",") {
    "array_agg(${it.name}) FILTER (where ${it.name} IS NOT NULL) as ${it.name}"
}

val jsonValueColumnsSql = PostgresDataTables.dataColumns.entries
        .joinToString(",") { (datatype, cols) ->
            val (ni, bt) = cols
            "COALESCE(jsonb_object_agg(${PROPERTY_TYPE_ID.name}, ${bt.name} || ${ni.name}) " +
                    "FILTER (WHERE ${bt.name} IS NOT NULL OR ${ni.name} IS NOT NULL ),'{}') " +
                    "as ${getDataColumnName(datatype)}"
        }

/**
 * Preparable SQL that selects entities grouping by id and property type id from the [DATA] table with the following
 * bind order:
 *
 * 1. entity set ids (array)
 * 2. entity key ids (array)
 * 3. partition (array)
 *
 */
internal val selectEntitiesGroupedByIdAndPropertyTypeId = "SELECT ${ENTITY_SET_ID.name}, ${ID_VALUE.name}, ${PARTITION.name}, ${PROPERTY_TYPE_ID.name}, $valuesColumnsSql FROM ${DATA.name} where ${ENTITY_SET_ID.name} = ANY(?) AND id = ANY(?) AND partition = ANY(?) GROUP BY (${ENTITY_SET_ID.name},${ID_VALUE.name}, ${PARTITION.name}, ${PROPERTY_TYPE_ID.name})"

/**
 * Preparable SQL that selects an entire entity set grouping by id and property type id from the [DATA] table with the following
 * bind order:
 *
 * 1. entity set id
 *
 */
internal val selectEntitySetGroupedByIdAndPropertyTypeId = "SELECT ${ENTITY_SET_ID.name}, ${ID_VALUE.name}, ${PARTITION.name}, ${PROPERTY_TYPE_ID.name}, $valuesColumnsSql FROM ${DATA.name} where ${ENTITY_SET_ID.name} = ANY(?) GROUP BY (${ENTITY_SET_ID.name},${ID_VALUE.name}, ${PARTITION.name}, ${PROPERTY_TYPE_ID.name})"
/**
 * Preparable SQL that selects entities grouping by id and property type id from the [DATA] table with the following
 * bind order:
 *
 * 1. entity set ids (array)
 * 2. entity key ids (array)
 * 3. partition (array)
 *
 */
internal val selectEntitiesSql = "SELECT ${ENTITY_SET_ID.name},${ID_VALUE.name},$jsonValueColumnsSql from ($selectEntitiesGroupedByIdAndPropertyTypeId) entities group by (${ENTITY_SET_ID.name},${ID_VALUE.name}, ${PARTITION.name})"

/**
 * Preparable SQL that selects an entire entity set grouping by id and property type id from the [DATA] table with the following
 * bind order:
 *
 * 1. entity set id
 *
 */
internal val selectEntitySetSql = "SELECT ${ENTITY_SET_ID.name},${ID_VALUE.name},$jsonValueColumnsSql from ($selectEntitySetGroupedByIdAndPropertyTypeId) entity_set group by (${ENTITY_SET_ID.name},${ID_VALUE.name}, ${PARTITION.name})"

/**
 * 1 - version
 * 2 - version
 * 3 - version
 * 4 - entity set id
 * 5 - entity key ids
 */
internal val upsertEntitiesSql = "UPDATE ${PostgresTable.ENTITY_KEY_IDS.name} SET ${VERSIONS.name} = ${VERSIONS.name} || ARRAY[?], ${DataTables.LAST_WRITE.name} = now(), " +
        "${VERSION.name} = CASE WHEN abs(${PostgresTable.ENTITY_KEY_IDS.name}.${VERSION.name}) < ? THEN $ " +
        "ELSE ${PostgresTable.ENTITY_KEY_IDS.name}.${VERSION.name} END " +
        "WHERE ${ENTITY_SET_ID.name} = ? AND ${ID_VALUE.name} = ANY(?)"


/**
 * Preparable sql to lock entities with the following bind order:
 * 1. entity set id
 * 2. entity key ids
 * 3. partition
 */
internal val lockEntitiesSql = "SELECT 1 FROM ${PostgresTable.ENTITY_KEY_IDS.name} " +
        "WHERE ${ENTITY_SET_ID.name} = ? AND ${ID_VALUE.name} = ANY(?) AND $PARTITION = ANY(?) AND ${PARTITIONS_VERSION.name} = ? " +
        "FOR UPDATE"


fun upsertEntities(entitySetId: UUID, idsClause: String, version: Long): String {
    return "UPDATE ${PostgresTable.ENTITY_KEY_IDS.name} SET ${VERSIONS.name} = ${VERSIONS.name} || ARRAY[$version], ${DataTables.LAST_WRITE.name} = now(), " +
            "${VERSION.name} = CASE WHEN abs(${PostgresTable.ENTITY_KEY_IDS.name}.${VERSION.name}) < $version THEN $version " +
            "ELSE ${PostgresTable.ENTITY_KEY_IDS.name}.${VERSION.name} END " +
            "WHERE ${ENTITY_SET_ID.name} = '$entitySetId' AND ${ID_VALUE.name} IN ($idsClause)"
}

/**
 * Preparable SQL that upserts a version for all entities in a given entity set in [PostgresTable.ENTITY_KEY_IDS]
 *
 * The following bind order is expected:
 *
 * 1. version
 * 2. version
 * 3. version
 * 4. entity set id
 */
internal val updateVersionsForEntitySet = "UPDATE ${PostgresTable.ENTITY_KEY_IDS.name} SET versions = versions || ARRAY[?]::uuid[], " +
        "${VERSION.name} = CASE WHEN abs(${PostgresTable.ENTITY_KEY_IDS.name}.${VERSION.name}) < ? THEN ? " +
        "ELSE ${PostgresTable.ENTITY_KEY_IDS.name}.${VERSION.name} END " +
        "WHERE ${ENTITY_SET_ID.name} = ? "

/**
 * Preparable SQL that upserts a version for all properties in a given entity set in [PostgresTable.DATA]
 *
 * The following bind order is expected:
 *
 * 1. version
 * 2. version
 * 3. version
 * 4. entity set id
 */
internal val updateVersionsForPropertiesInEntitySet = "UPDATE ${DATA.name} SET versions = versions || ARRAY[?]::uuid[], " +
        "${VERSION.name} = CASE WHEN abs(${PostgresTable.ENTITY_KEY_IDS.name}.${VERSION.name}) < ? THEN ? " +
        "ELSE ${PostgresTable.ENTITY_KEY_IDS.name}.${VERSION.name} END " +
        "WHERE ${ENTITY_SET_ID.name} = ? "


/**
 * Preparable SQL that upserts a version for all entities in a given entity set in [PostgresTable.ENTITY_KEY_IDS]
 *
 * The following bind order is expected:
 *
 * 1. version
 * 2. version
 * 3. version
 * 4. entity set id
 * 5. entity key ids
 * 6. partition
 * 7. partition version
 */
internal val updateVersionsForEntitiesInEntitySet = "$updateVersionsForEntitySet AND ${ID_VALUE.name} = ANY(?) " +
        "AND ${PARTITION.name} = ANY(?) AND ${PARTITIONS_VERSION.name} = ?"

/**
 * Preparable SQL thatupserts a version for all properties in a given entity set in [PostgresTable.DATA]
 *
 * The following bind order is expected:
 *
 * 1. version
 * 2. version
 * 3. version
 * 4. entity set id
 * 5. property type ids
 */
internal val updateVersionsForPropertyTypesInEntitySet = "$updateVersionsForPropertiesInEntitySet AND ${PROPERTY_TYPE_ID.name} = ANY(?)"

/**
 * Preparable SQL thatupserts a version for all properties in a given entity set in [PostgresTable.DATA]
 *
 * The following bind order is expected:
 *
 * 1. version
 * 2. version
 * 3. version
 * 4. entity set id
 * 5. entity key ids
 * 6. partition
 * 7. partition version
 */
internal val updateVersionsForPropertiesInEntitiesInEntitySet = "$updateVersionsForPropertiesInEntitySet AND ${ID_VALUE.name} = ANY(?) " +
        "AND PARTITION = ANY(?) AND ${PARTITIONS_VERSION.name} = ? "
/**
 * Preparable SQL thatpserts a version for all properties in a given entity set in [PostgresTable.DATA]
 *
 * The following bind order is expected:
 *
 * 1. version
 * 2. version
 * 3. version
 * 4. entity set id
 * 5. entity key ids
 * 6. partition
 * 7. partition version
 * 8. property type ids
 */
internal val updateVersionsForPropertyTypesInEntitiesInEntitySet = "$updateVersionsForPropertiesInEntitiesInEntitySet AND ${PROPERTY_TYPE_ID.name} = ANY(?)"

/**
 * Selects a text properties from entity sets with the following bind order:
 * 1. entity set ids  (array)
 * 2. property type ids (array)
 *
 */
internal val selectEntitySetTextProperties = "SELECT ${getDataColumnName(PostgresDatatype.TEXT)} FROM ${DATA.name} WHERE ${PostgresColumn.ENTITY_SET_ID.name} = ANY(?) AND $PROPERTY_TYPE_ID = ANY(?)"

/**
 * Selects a text properties from specific entities with the following bind order:
 * 1. entity set ids  (array)
 * 2. property type ids (array)
 * 3. entity key ids (array)
 */
internal val selectEntitiesTextProperties = "$selectEntitySetTextProperties AND $ID_VALUE = ANY(?)"

fun partitionSelectorFromId(entityKeyId: UUID): Int {
    return entityKeyId.leastSignificantBits.toInt()
}

fun getPartition(entityKeyId: UUID, partitions: List<Int>): Int {
    return partitions[partitionSelectorFromId(entityKeyId) % partitions.size]
}

/**
 * Builds the list of partitions for a given set of entity key ids.
 * @param entityKeyIds The entity key ids whose partitions will be retrieved.
 * @param partitions The partitions to select from.
 * @return A list of partitions.
 */
fun getPartitionsInfo(entityKeyIds: Set<UUID>, partitions: List<Int>): List<Int> {
    return entityKeyIds.map { entityKeyId -> getPartition(entityKeyId, partitions) }
}

/**
 * Builds a mapping of entity key id to partition.
 *
 * @param entityKeyIds The entity key ids whose partitions will be retrieved.
 * @param partitions The partitions to select from.
 *
 * @return A map of entity key ids to partitions.
 */
fun getPartitionsInfoMap(entityKeyIds: Set<UUID>, partitions: List<Int>): Map<UUID, Int> {
    return entityKeyIds.associateWith { entityKeyId -> getPartition(entityKeyId, partitions) }
}

fun getDataColumnName( datatype :PostgresDatatype ) : String {
    return "v_$datatype.name"
}
/**
 * This function generates preparable sql with the following bind order:
 *
 * 1.  ENTITY_SET_ID
 * 2.  ID_VALUE
 * 3.  PARTITION
 * 4.  PROPERTY_TYPE_ID
 * 5.  HASH
 * 6.  VERSION,
 * 7.  VERSIONS
 * 8.  PARTITIONS_VERSION
 * 9. Value Column
 */
fun upsertPropertyValueSql(propertyType: PropertyType): String {
    val insertColumn = getColumnDefinition(propertyType.postgresIndexType, propertyType.datatype)
    val metadataColumnsSql = listOf(
            ENTITY_SET_ID,
            ID_VALUE,
            PARTITION,
            PROPERTY_TYPE_ID,
            HASH,
            LAST_WRITE,  // Will get set to now()
            VERSION,
            VERSIONS,
            PARTITIONS_VERSION
    ).joinToString(",")
    return "INSERT INTO ${DATA.name} ($metadataColumnsSql,${insertColumn.name}) VALUES (?,?,?,?,?,now(),?,?,?,?) " +
            "ON CONFLICT (${ENTITY_SET_ID.name},${ID_VALUE.name}, ${HASH.name}) DO UPDATE " +
            "SET ${VERSIONS.name} = ${DATA.name}.${VERSIONS.name} || EXCLUDED.${VERSIONS.name}, " +
            "${LAST_WRITE.name} = GREATEST(${LAST_WRITE.name},EXCLUDED.${LAST_WRITE.name}), " +
            "${PARTITIONS_VERSION.name} = EXCLUDED.${PARTITIONS_VERSION.name}, " +
            "${VERSION.name} = CASE WHEN abs(${DATA.name}.${VERSION.name}) < EXCLUDED.${VERSION.name} THEN EXCLUDED.${VERSION.name} " +
            "ELSE ${DATA.name}.${VERSION.name} END"
}


