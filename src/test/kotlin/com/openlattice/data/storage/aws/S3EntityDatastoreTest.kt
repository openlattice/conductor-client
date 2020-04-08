package com.openlattice.data.storage.aws

import com.geekbeast.rhizome.aws.newS3Client
import com.google.common.util.concurrent.MoreExecutors
import com.kryptnostic.rhizome.configuration.amazon.AwsLaunchConfiguration
import com.kryptnostic.rhizome.pods.AwsConfigurationLoader
import com.openlattice.aws.*
import com.openlattice.data.storage.ByteBlobDataManager
import com.openlattice.data.storage.LocalBlobDataService
import com.openlattice.datastore.configuration.DatastoreConfiguration
import com.openlattice.datastore.configuration.S3StorageConfiguration
import com.openlattice.mapstores.TestDataFactory
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind
import org.junit.Assert
import org.junit.Test
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
        private val byteBlobDataManager = AwsBlobDataService(
                datastoreConfiguration,
                MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(2))
        )
        private val s3EntityDatastore = s3StorageConfiguration.getWriter(byteBlobDataManager)
        private val s3EntityLoader = s3StorageConfiguration.getLoader(byteBlobDataManager)
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
}