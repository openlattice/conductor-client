package com.openlattice.data.util

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.openlattice.data.storage.ByteBlobDataManager
import com.openlattice.data.storage.MetadataOption
import com.openlattice.edm.EdmConstants
import com.openlattice.edm.type.PropertyType
import com.openlattice.postgres.PostgresMetaDataProperties
import com.openlattice.postgres.PostgresMetaDataProperties.LAST_WRITE
import com.openlattice.postgres.ResultSetAdapters
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind
import org.apache.olingo.commons.api.edm.FullQualifiedName
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
fun readJsonEntity(
        mapper: ObjectMapper,
        json: String,
        propertyTypes: Map<UUID, PropertyType>,
        byteBlobDataManager: ByteBlobDataManager
): MutableMap<UUID, MutableSet<Any>> {
    val entity = mapper.readValue<MutableMap<UUID, MutableSet<Any>>>(json)
    // Note: this call deletes all entries from result, which is not in propertyTypes (ID for example)
    (entity.keys - propertyTypes.keys).forEach { entity.remove(it) }

    propertyTypes.forEach { (_, propertyType) ->

        if (propertyType.datatype == EdmPrimitiveTypeKind.Binary) {
            val urls = entity.getOrElse(propertyType.id) { mutableSetOf() }
            if (urls.isNotEmpty()) {
                entity[propertyType.id] = byteBlobDataManager.getObjects(urls).toMutableSet()
            }
        }
    }

    return entity
}

fun readJsonEntity(
        mapper: ObjectMapper,
        json: ByteArray,
        propertyTypes: Map<UUID, PropertyType>,
        byteBlobDataManager: ByteBlobDataManager
): MutableMap<UUID, MutableSet<Any>> {
    val entity = mapper.readValue<MutableMap<UUID, MutableSet<Any>>>(json)
    // Note: this call deletes all entries from result, which is not in propertyTypes (ID for example)
    (entity.keys - propertyTypes.keys).forEach { entity.remove(it) }

    propertyTypes.forEach { (_, propertyType) ->

        if (propertyType.datatype == EdmPrimitiveTypeKind.Binary) {
            val urls = entity.getOrElse(propertyType.id) { mutableSetOf() }
            if (urls.isNotEmpty()) {
                entity[propertyType.id] = byteBlobDataManager.getObjects(urls).toMutableSet()
            }
        }
    }

    return entity
}

fun mapEntityKeyIdsToFqns(
        entity: Map<UUID, MutableSet<Any>>,
        propertyTypes: Map<UUID, PropertyType>
): MutableMap<FullQualifiedName, MutableSet<Any>> {
    return  entity.mapKeys { propertyTypes.getValue(it.key).type }.toMutableMap()
}

