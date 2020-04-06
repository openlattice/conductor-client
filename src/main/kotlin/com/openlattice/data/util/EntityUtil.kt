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
    return readEntity(entity, propertyTypes, byteBlobDataManager)
}

fun readJsonEntity(
        mapper: ObjectMapper,
        json: ByteArray,
        propertyTypes: Map<UUID, PropertyType>,
        byteBlobDataManager: ByteBlobDataManager
): MutableMap<UUID, MutableSet<Any>> {
    val entity = mapper.readValue<MutableMap<UUID, MutableSet<Any>>>(json)
    return readEntity(entity, propertyTypes, byteBlobDataManager)
}

/**
 * Reads an entity and filters are all property types that weren't requested.
 */
fun readEntity(
        entity: MutableMap<UUID, MutableSet<Any>>,
        propertyTypes: Map<UUID, PropertyType>,
        byteBlobDataManager: ByteBlobDataManager
): MutableMap<UUID, MutableSet<Any>> {

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
    return entity.mapKeys { propertyTypes.getValue(it.key).type }.toMutableMap()
}

internal fun mapMetadataOptionsToPropertyTypes(metadataOptions: Set<MetadataOption>): Map<UUID, PropertyType> {
    //TODO: Consider turning ID into default metadata option that is included in all requests
    return metadataOptions.map {
        when (it) {
            MetadataOption.LAST_WRITE -> PostgresMetaDataProperties.LAST_WRITE.propertyType.id to PostgresMetaDataProperties.LAST_WRITE.propertyType
            MetadataOption.VERSION -> PostgresMetaDataProperties.VERSION.propertyType.id to PostgresMetaDataProperties.VERSION.propertyType
            else -> throw IllegalArgumentException("Unsupported metadata for s3.")
        }
    }.toMap()
}