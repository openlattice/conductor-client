package com.openlattice.data.storage.aws

import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.model.PutObjectRequest
import com.amazonaws.services.s3.model.StorageClass
import com.dataloom.mappers.ObjectMappers
import com.fasterxml.jackson.module.kotlin.readValue
import com.geekbeast.rhizome.aws.newS3Client
import com.geekbeast.util.LinearBackoff
import com.geekbeast.util.attempt
import com.hazelcast.config.MapConfig
import com.hazelcast.config.MapStoreConfig
import com.kryptnostic.rhizome.mapstores.TestableSelfRegisteringMapStore
import com.openlattice.IdConstants
import com.openlattice.data.Entity
import com.openlattice.data.EntityDataKey
import com.openlattice.data.Property
import com.openlattice.data.storage.S3StorageConfiguration
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.mapstores.TestDataFactory
import com.openlattice.postgres.mapstores.TypedMapIdentifier
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind
import java.io.ByteArrayInputStream
import java.time.OffsetDateTime
import java.util.*
import java.util.stream.Collectors

private const val S3_OBJECT_MAP_TTL = 5 * 60 // 5 minutes in seconds

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class S3ObjectMapStore(
        private val s3StorageConfiguration: S3StorageConfiguration,
        private val identifier: TypedMapIdentifier<EntityDataKey, Entity> = HazelcastMap.S3_OBJECT_STORE
) : TestableSelfRegisteringMapStore<EntityDataKey, Entity> {

    private val s3 = newS3Client(
            s3StorageConfiguration.accessKeyId,
            s3StorageConfiguration.secretAccessKey,
            s3StorageConfiguration.region
    )

    private val mapper = ObjectMappers.newJsonMapper()

    override fun storeAll(entities: Map<EntityDataKey, Entity>) {
        val lastWrite = OffsetDateTime.now()
        val version = lastWrite.toInstant().toEpochMilli()

        entities.entries.parallelStream().forEach { (ewk, data) ->
            attempt(LinearBackoff(5000, 100), 10) {
                data.getOrPut(IdConstants.LAST_WRITE_ID.id) { mutableSetOf() }.add(Property(lastWrite, propertyMetadata = mapOf()) )
                data.getOrPut(IdConstants.VERSION_ID.id) { mutableSetOf() }.add(Property(version, propertyMetadata = mapOf()) )
                val json = mapper.writeValueAsBytes(data)
                val metadata = ObjectMetadata()
                metadata.contentLength = json.size.toLong()

                val putRequest = PutObjectRequest(
                        s3StorageConfiguration.bucket,
                        getS3Key(ewk.entitySetId, ewk.entityKeyId),
                        ByteArrayInputStream(json), metadata
                )
                putRequest.setStorageClass(StorageClass.StandardInfrequentAccess)
                s3.putObject(putRequest)
            }
        }
    }

    override fun loadAllKeys(): MutableIterable<EntityDataKey>? {
        return null
    }

    override fun store(key: EntityDataKey, value: Entity) {
        storeAll(mapOf(key to value))
    }

    override fun loadAll(keys: Collection<EntityDataKey>): Map<EntityDataKey, Entity> {
        return keys
                .parallelStream()
                .map { edk ->
                    val json = attempt(LinearBackoff(5000, 100), 10) {
                        s3
                                .getObject(s3StorageConfiguration.bucket, getS3Key(edk.entitySetId, edk.entityKeyId))
                                .objectContent
                                .readAllBytes()

                    }
                    edk to Entity(mapper.readValue(json))
                }.collect(Collectors.toConcurrentMap({ it.first }, { it.second }))

    }

    override fun deleteAll(keys: Collection<EntityDataKey>) {
        return keys.parallelStream().forEach { edk ->
            attempt(LinearBackoff(5000, 100), 10) {
                s3.deleteObject(s3StorageConfiguration.bucket, getS3Key(edk.entitySetId, edk.entityKeyId))
            }
        }
    }

    override fun load(key: EntityDataKey): Entity? {
        return loadAll(listOf(key))[key]
    }

    override fun delete(key: EntityDataKey) {
        deleteAll(listOf(key))
    }


    private fun getS3Key(entitySetId: UUID, id: UUID): String {
        return "$entitySetId/$id/data"
    }

    override fun getMapName(): String = identifier.name()


    override fun getTable(): String = identifier.name()

    override fun generateTestKey(): EntityDataKey {
        return TestDataFactory.entityDataKey()
    }

    override fun generateTestValue(): Entity {
        val propertyTypes = setOf(
                TestDataFactory.propertyType(EdmPrimitiveTypeKind.String),
                TestDataFactory.propertyType(EdmPrimitiveTypeKind.String),
                TestDataFactory.propertyType(EdmPrimitiveTypeKind.String)
        ).associateBy { it.id }
        return Entity(
                TestDataFactory.entities(
                        1,
                        propertyTypes
                ).values.map {
                    it.mapValues { (_, values) ->
                        values.map { v ->
                            Property(
                                    v,
                                    propertyMetadata = mapOf()
                            )
                        }.toMutableSet()
                    }.toMutableMap()
                }.first()
        )
    }

    override fun getMapStoreConfig(): MapStoreConfig {
        return MapStoreConfig()
                .setInitialLoadMode(MapStoreConfig.InitialLoadMode.EAGER)
                .setImplementation(this)
                .setEnabled(true)
                .setWriteDelaySeconds(0)
    }

    override fun getMapConfig(): MapConfig {
        return MapConfig(mapName)
                .setMapStoreConfig(mapStoreConfig)
                .setTimeToLiveSeconds(S3_OBJECT_MAP_TTL)
    }
}