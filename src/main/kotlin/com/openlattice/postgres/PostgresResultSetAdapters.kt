package com.openlattice.postgres

import com.dataloom.mappers.ObjectMappers
import com.fasterxml.jackson.module.kotlin.readValue
import com.openlattice.IdConstants.LAST_WRITE_ID
import com.openlattice.data.storage.ByteBlobDataManager
import com.openlattice.data.storage.MetadataOption
import com.openlattice.data.storage.PROPERTIES
import com.openlattice.data.storage.VALUE
import com.openlattice.edm.EdmConstants.Companion.ID_PROPERTY_TYPE
import com.openlattice.edm.EdmConstants.Companion.LAST_WRITE_PROPERTY_TYPE
import com.openlattice.edm.type.PropertyType
import com.openlattice.postgres.ResultSetAdapters.*
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind
import org.slf4j.LoggerFactory
import java.sql.*
import java.sql.Date
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneId
import java.util.*
import kotlin.collections.LinkedHashSet


internal class PostgresResultSetAdapters

private val logger = LoggerFactory.getLogger(PostgresResultSetAdapters::class.java)
private val mapper = ObjectMappers.newJsonMapper()

/**
 * Returns linked entity data from the [ResultSet] mapped respectively by its id, entity set, origin id and property
 * type key (as specified by [keyMapper]).
 * Note: Do not include the linking id for the [IdConstants.ID_ID] key as a property for this adapter, because it is
 * used for linked entity indexing and we preserve that key for the origin id.
 * @see ConductorElasticsearchImpl.formatLinkedEntity
 */
@Throws(SQLException::class)
fun <T> getLinkingEntityData(
        rs: ResultSet,
        authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
        metadataOptions: Set<MetadataOption>,
        byteBlobDataManager: ByteBlobDataManager,
        keyMapper: (PropertyType) -> T
): Pair<UUID, Pair<UUID, Map<UUID, MutableMap<T, MutableSet<Any>>>>> {
    val id = id(rs)
    val entitySetId = entitySetId(rs)
    val propertyTypes = authorizedPropertyTypes.getValue(entitySetId)

    val lastWrite = if (metadataOptions.contains(MetadataOption.LAST_WRITE)) {
        Optional.of(lastWriteTyped(rs))
    } else {
        Optional.empty()
    }

    val entities = readLinkingJsonDataColumns(rs, propertyTypes, byteBlobDataManager, lastWrite)
            .mapValues { (_, propertyValues) ->
                propertyValues.mapKeys {
                    if (it.key == LAST_WRITE_ID.id) {
                        keyMapper(LAST_WRITE_PROPERTY_TYPE)
                    } else {
                        keyMapper(propertyTypes.getValue(it.key))
                    }
                }.toMutableMap()
            }

    return id to (entitySetId to entities)
}

@Throws(SQLException::class)
fun <T> getEntityData(
        rs: ResultSet,
        authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
        metadataOptions: Set<MetadataOption>,
        includeEntityKeyId: Boolean = false,
        keyMapper: (PropertyType) -> T
): Pair<UUID, MutableMap<T, MutableSet<Any>>> {
    val id = id(rs)
    val entitySetId = entitySetId(rs)
    val propertyTypes = authorizedPropertyTypes.getValue(entitySetId)

    val entity = readJsonDataColumns(rs, propertyTypes)
            .mapKeys { keyMapper(propertyTypes.getValue(it.key)) }
            .toMutableMap()

    if (includeEntityKeyId) {
        entity[keyMapper(ID_PROPERTY_TYPE)] = mutableSetOf<Any>(id.toString())
    }

    if (metadataOptions.contains(MetadataOption.LAST_WRITE)) {
        entity[keyMapper(LAST_WRITE_PROPERTY_TYPE)] = mutableSetOf<Any>(lastWriteTyped(rs))
    }

    return id to entity
}

fun <T> transformBinaryDataInEntityMap(
        entityMap: Map<UUID, MutableMap<T, MutableSet<Any>>>,
        propertyTypes: Map<UUID, Map<UUID, PropertyType>>,
        byteBlobDataManager: ByteBlobDataManager,
        keyMapper: (PropertyType) -> T
): Map<UUID, MutableMap<T, MutableSet<Any>>> {
    val binaryPropertyTypeKeys = propertyTypes
            .values
            .flatMap { it.values }
            .filter { it.datatype == EdmPrimitiveTypeKind.Binary }
            .map { keyMapper(it) }
            .toSet()

    if (binaryPropertyTypeKeys.isEmpty()) {
        return entityMap
    }

    val urls = entityMap.values.flatMapTo(LinkedHashSet<Any>()) { entity ->
        entity.filter { binaryPropertyTypeKeys.contains(it.key)}.values
    }

    if (urls.isEmpty()) {
        return entityMap
    }

    val presignedUrlsMap = byteBlobDataManager.getObjectsAsMap(urls)

    return entityMap.mapValues { entry ->
        val entity = entry.value
        binaryPropertyTypeKeys.forEach { fqn ->
            entity[fqn]?.let { entity[fqn] = it.mapNotNullTo(LinkedHashSet(it.size)) { url -> presignedUrlsMap[url] } }
        }
        entity
    }
}

@Throws(SQLException::class)
fun readJsonDataColumns(
        rs: ResultSet,
        propertyTypes: Map<UUID, PropertyType>
): MutableMap<UUID, MutableSet<Any>> {
    val entity = mapper.readValue<MutableMap<UUID, MutableSet<Any>>>(rs.getString(PROPERTIES))

    // Note: this call deletes all entries from result, which is not in propertyTypes (ID for example)
    (entity.keys - propertyTypes.keys).forEach { entity.remove(it) }

    return entity
}

@Throws(SQLException::class)
fun readLinkingJsonDataColumns(
        rs: ResultSet,
        propertyTypes: Map<UUID, PropertyType>,
        byteBlobDataManager: ByteBlobDataManager,
        lastWrite: Optional<OffsetDateTime>
): MutableMap<UUID, MutableMap<UUID, MutableSet<Any>>> {
    val lastWriteIncluded = lastWrite.isPresent

    val detailedEntity = mapper.readValue<MutableMap<UUID, MutableSet<MutableMap<String, Any>>>>(rs.getString(PROPERTIES))

    val entities = mutableMapOf<UUID, MutableMap<UUID, MutableSet<Any>>>() // origin id -> property type id -> values
    detailedEntity.forEach { (propertyTypeId, details) ->
        // only select properties which are authorized
        if (propertyTypes.keys.contains(propertyTypeId)) {

            details.forEach { entityDetail ->
                val originId = UUID.fromString(entityDetail[PostgresColumn.ID_VALUE.name] as String)
                val propertyValue = entityDetail.getValue(VALUE)

                if (!entities.containsKey(originId)) {
                    entities[originId] = mutableMapOf(propertyTypeId to mutableSetOf(propertyValue))
                } else {
                    entities.getValue(originId)[propertyTypeId] = mutableSetOf(propertyValue)
                }

                if (lastWriteIncluded) {
                    entities.getValue(originId)[LAST_WRITE_ID.id] = mutableSetOf<Any>(lastWrite.get())
                }
            }
        }
    }

    propertyTypes.forEach { (_, propertyType) ->
        if (propertyType.datatype == EdmPrimitiveTypeKind.Binary) {
            entities.forEach { (_, entity) ->
                val urls = entity.getOrElse(propertyType.id) { mutableSetOf() }
                if (urls.isNotEmpty()) {
                    entity[propertyType.id] = byteBlobDataManager.getObjects(urls).toMutableSet()
                }
            }
        }
    }

    return entities
}
