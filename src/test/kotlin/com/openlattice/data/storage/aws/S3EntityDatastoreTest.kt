package com.openlattice.data.storage.aws

import com.codahale.metrics.MetricRegistry
import com.geekbeast.rhizome.aws.S3FolderListingIterable
import com.geekbeast.rhizome.aws.S3ObjectListingIterable
import com.geekbeast.rhizome.aws.newS3Client
import com.geekbeast.rhizome.hazelcast.mockHazelcastMap
import com.google.common.util.concurrent.MoreExecutors
import com.hazelcast.core.HazelcastInstance
import com.kryptnostic.rhizome.configuration.amazon.AwsLaunchConfiguration
import com.kryptnostic.rhizome.pods.AwsConfigurationLoader
import com.openlattice.aws.*
import com.openlattice.data.Entity
import com.openlattice.data.EntityDataKey
import com.openlattice.data.storage.EntityLoader
import com.openlattice.data.storage.S3StorageConfiguration
import com.openlattice.datastore.configuration.DatastoreConfiguration
import com.openlattice.data.storage.providers.S3StorageProvider
import com.openlattice.mapstores.TestDataFactory
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind
import org.junit.Assert
import org.junit.Test
import org.mockito.Mockito
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.Executors
import kotlin.streams.toList

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class S3EntityDatastoreTest {

    companion object {
        private val buildS3 = newS3Client(getBuildProfile(), getBuildRegion())
        private val awsConfigurationLoader = AwsConfigurationLoader(
                buildS3,
                AwsLaunchConfiguration(
                        getBuildBucket(),
                        Optional.of("conductor-client-tests"),
                        Optional.empty()
                )
        )
        private val s3StorageConfiguration = awsConfigurationLoader.load(S3StorageConfiguration::class.java)
        private val datastoreConfiguration = awsConfigurationLoader.load(DatastoreConfiguration::class.java)
        private val hazelcastInstance = Mockito.mock(HazelcastInstance::class.java)
        private val metricRegistry = MetricRegistry()
        private val s3StorageProvider: S3StorageProvider

        private val byteBlobDataManager = AwsBlobDataService(
                datastoreConfiguration,
                MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(2))
        )

        init {
            val s3Objects = mockHazelcastMap(EntityDataKey::class.java, Entity::class.java)
            s3StorageProvider = S3StorageProvider(
                    hazelcastInstance,
                    byteBlobDataManager,
                    metricRegistry,
                    s3StorageConfiguration
            )

            Mockito.`when`(hazelcastInstance.getMap<EntityDataKey, Entity>("S3_OBJECT_STORE")).thenReturn(s3Objects)

        }


        private val s3EntityDatastore = s3StorageProvider.entityWriter as S3EntityDatastore
        private val s3EntityLoader = s3StorageProvider.entityLoader as EntityLoader
        private val logger: Logger = LoggerFactory.getLogger(S3EntityDatastoreTest::class.java)
    }


    @Test
    fun testReadWrite() {
        val propertyTypes = setOf(
                TestDataFactory.propertyType(EdmPrimitiveTypeKind.String),
                TestDataFactory.propertyType(EdmPrimitiveTypeKind.String),
                TestDataFactory.propertyType(EdmPrimitiveTypeKind.String)
        ).associateBy { it.id }

        val entities = TestDataFactory.entities(10, propertyTypes)
        val entitySetId = UUID.randomUUID()

        val writeEvent = s3EntityDatastore.createOrUpdateEntities(entitySetId, entities, propertyTypes)
        val loaded = s3EntityLoader.getEntitiesWithMetadata(
                entitySetId,
                entities.keys,
                mutableMapOf(entitySetId to propertyTypes)
        ).toList()

        Assert.assertEquals(entities.size, loaded.size)
    }

    @Test
    fun testListEntities() {
        S3FolderListingIterable<UUID>(
                s3EntityDatastore.s3,
                s3StorageConfiguration.bucket,
                "3e474756-4ac1-40f2-9432-ffc96b9259a0/",
                2
        ) {
            UUID.fromString(it)
        }.forEach { folder ->
            logger.info(folder.toString())
            S3ObjectListingIterable(
                    s3EntityDatastore.s3, s3StorageConfiguration.bucket,
                    "3e474756-4ac1-40f2-9432-ffc96b9259a0/$folder/"
            ) { s -> s }.forEach { obj -> logger.info("\t$obj") }
        }
//
//        val listObjReq = ListObjectsV2Request()
//                .withBucketName(s3StorageConfiguration.bucket)
//                .withMaxKeys(20)
//                .withPrefix("3e474756-4ac1-40f2-9432-ffc96b9259a0/2512ec7b-0285-4395-a20f-af014131ff10/")
//                .withDelimiter("/")
//
//
//        val rawResult = s3EntityDatastore
//                .s3
//                .listObjectsV2(listObjReq)
//        val result = rawResult.commonPrefixes
//
//        result.replaceAll { it.removePrefix("3e474756-4ac1-40f2-9432-ffc96b9259a0/") }
//
//
//
//        result.forEach { logger.info(it) }
    }
}