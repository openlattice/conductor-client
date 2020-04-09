package com.openlattice.data.storage.aws

import com.amazonaws.services.s3.model.DeleteObjectRequest
import com.amazonaws.services.s3.model.DeleteObjectsRequest
import com.amazonaws.services.s3.model.ListObjectsV2Request
import com.amazonaws.services.s3.transfer.TransferManagerBuilder
import com.codahale.metrics.MetricRegistry
import com.codahale.metrics.Timer
import com.dataloom.mappers.ObjectMappers
import com.geekbeast.rhizome.aws.S3FolderListingIterable
import com.geekbeast.rhizome.aws.newS3Client
import com.hazelcast.core.IMap
import com.openlattice.IdConstants
import com.openlattice.data.*
import com.openlattice.data.storage.ByteBlobDataManager
import com.openlattice.data.storage.EntityDatastore
import com.openlattice.data.storage.MetadataOption
import com.openlattice.data.storage.PostgresEntityDatastore
import com.openlattice.data.util.mapEntityKeyIdsToFqns
import com.openlattice.data.util.mapMetadataOptionsToPropertyTypes
import com.openlattice.data.util.readEntity
import com.openlattice.datastore.configuration.S3StorageConfiguration
import com.openlattice.edm.EdmConstants
import com.openlattice.edm.type.PropertyType
import com.openlattice.postgres.streams.BasePostgresIterable
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.lang.Exception
import java.nio.ByteBuffer
import java.time.OffsetDateTime
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.Semaphore
import java.util.stream.Stream
import kotlin.streams.asStream


/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Service
class S3EntityDatastore(
        private val s3StorageConfiguration: S3StorageConfiguration,
        private val byteBlobDataManager: ByteBlobDataManager,
        private val s3ObjectStore: IMap<EntityDataKey, Entity>,
        metricRegistry: MetricRegistry
) : EntityDatastore {
    companion object {
        private val logger = LoggerFactory.getLogger(S3EntityDatastore::class.java)
    }


    internal val s3 = newS3Client(
            s3StorageConfiguration.accessKeyId,
            s3StorageConfiguration.secretAccessKey,
            s3StorageConfiguration.region
    )

    private val executor = Executors.newFixedThreadPool(s3StorageConfiguration.threads)
    private val semaphore = Semaphore(10000)
    private val transferManager = TransferManagerBuilder.standard().withS3Client(s3)
    private val mapper = ObjectMappers.newJsonMapper()

    override val entitiesTimer: Timer = metricRegistry.timer(
            MetricRegistry.name(
                    PostgresEntityDatastore::class.java,
                    "getEntities"
            )
    )
    override val linkedEntitiesTimer: Timer = metricRegistry.timer(
            MetricRegistry.name(
                    PostgresEntityDatastore::class.java,
                    "getEntities(linked)"
            )
    )

    override fun getAllEntitiesWithMetadata(
            entitySetId: UUID, authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
            metadataOptions: EnumSet<MetadataOption>
    ): Stream<MutableMap<FullQualifiedName, MutableSet<Any>>> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getEntitiesAcrossEntitySets(
            entityKeyIds: Map<UUID, Optional<Set<UUID>>>, authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
            metadataOptions: EnumSet<MetadataOption>, linking: Boolean
    ): Iterable<Pair<UUID, MutableMap<FullQualifiedName, MutableSet<Any>>>> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    private fun getS3Key(entitySetId: UUID, id: UUID): String {
        return "$entitySetId/$id/data"
    }

    override fun getEntitiesWithMetadata(
            entitySetId: UUID,
            ids: Set<UUID>,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
            metadataOptions: EnumSet<MetadataOption>
    ): Stream<MutableMap<FullQualifiedName, MutableSet<Any>>> {
        val propertyTypes =
                authorizedPropertyTypes.getValue(entitySetId) + mapMetadataOptionsToPropertyTypes(metadataOptions)

        //Consider parallelizing
        return s3ObjectStore
                .getAll(ids.map { EntityDataKey(entitySetId, it) }.toSet())
                .asSequence()
                .map { (edk, entity) ->
                    val entityByFqn = mapEntityKeyIdsToFqns(
                            readEntity(entity, propertyTypes, byteBlobDataManager),
                            propertyTypes
                    )
                    entityByFqn[EdmConstants.ID_FQN] = mutableSetOf<Any>(edk.entitySetId)
                    return@map entityByFqn
                }
                .asStream()
    }

    override fun getLinkingEntities(
            entityKeyIds: Map<UUID, Optional<Set<UUID>>>,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>
    ): Stream<MutableMap<FullQualifiedName, MutableSet<Any>>> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getLinkingEntitiesWithMetadata(
            entityKeyIds: Map<UUID, Optional<Set<UUID>>>,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
            metadataOptions: EnumSet<MetadataOption>
    ): Stream<MutableMap<FullQualifiedName, MutableSet<Any>>> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getLinkedEntityDataByLinkingIdWithMetadata(
            linkingIdsByEntitySetId: Map<UUID, Optional<Set<UUID>>>,
            authorizedPropertyTypesByEntitySetId: Map<UUID, Map<UUID, PropertyType>>,
            extraMetadataOptions: EnumSet<MetadataOption>
    ): Map<UUID, Map<UUID, Map<UUID, Map<UUID, Set<Any>>>>> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getLinkedEntitySetBreakDown(
            linkingIdsByEntitySetId: Map<UUID, Optional<Set<UUID>>>,
            authorizedPropertyTypesByEntitySetId: Map<UUID, Map<UUID, PropertyType>>
    ): Map<UUID, Map<UUID, Map<UUID, Map<FullQualifiedName, Set<Any>>>>> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getEntityKeyIdsOfLinkingIds(
            linkingIds: Set<UUID>, normalEntitySetIds: Set<UUID>
    ): BasePostgresIterable<Pair<UUID, Set<UUID>>> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun createOrUpdateEntities(
            entitySetId: UUID,
            entities: Map<UUID, MutableMap<UUID, MutableSet<Any>>>,
            authorizedPropertyTypes: Map<UUID, PropertyType>,
            versioned: Boolean
    ): WriteEvent {
        val lastWrite = OffsetDateTime.now()
        val version = lastWrite.toInstant().toEpochMilli()

        //TODO: Use versioned and unversioned s3 object store.

        entities.entries.parallelStream().map { (id, data) ->
            data.putIfAbsent(IdConstants.LAST_WRITE_ID.id, mutableSetOf(lastWrite))
            data.putIfAbsent(IdConstants.VERSION_ID.id, mutableSetOf(version))
            s3ObjectStore.setAsync(EntityDataKey(entitySetId, id), Entity(data))
        }.forEach {
            try {
                it.get()
            } catch (ex: Exception) {
                logger.error("Unable to write entity.", ex)
                throw ex
            }
        }

        return WriteEvent(version, entities.size)
    }

    override fun replaceEntities(
            entitySetId: UUID,
            entities: Map<UUID, MutableMap<UUID, MutableSet<Any>>>,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): WriteEvent {
        return performUpdate(entitySetId, entities, authorizedPropertyTypes, UpdateType.Replace)
    }

    fun performUpdate(
            entitySetId: UUID,
            entities: Map<UUID, MutableMap<UUID, MutableSet<Any>>>,
            authorizedPropertyTypes: Map<UUID, PropertyType>,
            updateType: UpdateType
    ): WriteEvent {
        val lastWrite = OffsetDateTime.now()
        val version = lastWrite.toInstant().toEpochMilli()

        entities.map { (id, entity) ->
            entity.putIfAbsent(IdConstants.LAST_WRITE_ID.id, mutableSetOf(lastWrite))
            entity.putIfAbsent(IdConstants.VERSION_ID.id, mutableSetOf(version))
            s3ObjectStore.submitToKey(
                    EntityDataKey(entitySetId, id),
                    S3EntityUpdateEntryProcessor(entity, UpdateType.PartialReplace)
            )
        }.forEach {
            try {
                it.get()
            } catch (ex: Exception) {
                logger.error(
                        "Unable to perform update of type {} for entity set {} with ids {}",
                        updateType,
                        entitySetId,
                        entities.keys,
                        ex
                )
                throw ex
            }
        }

        return WriteEvent(version, entities.size)
    }

    override fun partialReplaceEntities(
            entitySetId: UUID,
            entities: Map<UUID, MutableMap<UUID, MutableSet<Any>>>,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): WriteEvent {
        return performUpdate(entitySetId, entities, authorizedPropertyTypes, UpdateType.PartialReplace)
    }

    override fun replacePropertiesInEntities(
            entitySetId: UUID,
            replacementProperties:
            Map<UUID, Map<UUID, Set<Map<ByteBuffer, Any>>>>,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): WriteEvent {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun clearEntitySet(
            entitySetId: UUID,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): WriteEvent {
        val lastWrite = OffsetDateTime.now()
        val version = lastWrite.toInstant().toEpochMilli()

        val keys = S3FolderListingIterable<String>(s3,s3StorageConfiguration.bucket, "$entitySetId/") {
            it
        }.toList().toTypedArray()
        val deleteRequest = DeleteObjectsRequest(s3StorageConfiguration.bucket)
                .withKeys(*keys)

        val deletionResult = s3.deleteObjects(deleteRequest)

        return WriteEvent(version, deletionResult.deletedObjects.size)
    }

    override fun clearEntities(
            entitySetId: UUID,
            entityKeyIds: Set<UUID>,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): WriteEvent {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun clearEntityProperties(
            entitySetId: UUID,
            entityKeyIds: Set<UUID>,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): WriteEvent {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun deleteEntitySetData(
            entitySetId: UUID,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): WriteEvent {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun deleteEntities(
            entitySetId: UUID,
            entityKeyIds: Set<UUID>,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): WriteEvent {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun deleteEntityProperties(
            entitySetId: UUID,
            entityKeyIds: Set<UUID>,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): WriteEvent {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getExpiringEntitiesFromEntitySet(
            entitySetId: UUID,
            expirationBaseColumn: String,
            formattedDateMinusTTE: Any,
            sqlFormat: Int,
            deleteType: DeleteType
    ): BasePostgresIterable<UUID> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}

