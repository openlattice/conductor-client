package com.openlattice.data

import com.amazonaws.regions.Region
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.geekbeast.util.StopWatch
import com.google.common.util.concurrent.MoreExecutors
import com.kryptnostic.rhizome.configuration.amazon.AmazonLaunchConfiguration
import com.kryptnostic.rhizome.configuration.amazon.AwsLaunchConfiguration
import com.openlattice.ResourceConfigurationLoader
import com.openlattice.data.storage.ByteBlobDataManager
import com.openlattice.data.storage.aws.AwsBlobDataService
import com.openlattice.datastore.configuration.DatastoreConfiguration
import org.junit.BeforeClass
import org.junit.Ignore
import org.junit.Test
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.net.URL
import java.util.*
import java.util.concurrent.Executors
import kotlin.math.pow
import kotlin.math.sqrt

class S3BenchmarkTest {
    private val logger: Logger = LoggerFactory.getLogger(S3BenchmarkTest::class.java)
    private val NUM_BYTES = 30*1024*1024
    private val NUM_OBJECTS = 50

    companion object {
        @JvmStatic
        private lateinit var byteBlobDataManager: ByteBlobDataManager
        private var keys = mutableListOf<String>()

        @BeforeClass
        @JvmStatic
        fun setUp() {
            //val datastoreConfig = setUpAws()
            val datastoreConfig = setUpLocal()
            val byteBlobDataManager = AwsBlobDataService(
                    datastoreConfig,
                    MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(2))
            )
            this.byteBlobDataManager = byteBlobDataManager
        }

        private fun setUpLocal(): DatastoreConfiguration {
            return ResourceConfigurationLoader.loadConfiguration(DatastoreConfiguration::class.java)
        }

        private fun setUpAws(): DatastoreConfiguration {
            val awsTestConfig = ResourceConfigurationLoader
                    .loadConfigurationFromResource("awstest.yaml", AwsLaunchConfiguration::class.java)
            val s3 = getS3Client(awsTestConfig)
            return ResourceConfigurationLoader.loadConfigurationFromS3(
                    s3,
                    awsTestConfig.bucket,
                    awsTestConfig.folder,
                    DatastoreConfiguration::class.java)
        }

        private fun getS3Client(config: AmazonLaunchConfiguration): AmazonS3 {
            val builder = AmazonS3ClientBuilder.standard()
            builder.region = Region.getRegion(config.region.or(Regions.DEFAULT_REGION)).name
            return builder.build()
        }
    }

    @Test
    @Ignore
    fun benchmarkS3() {
        val data = generateTestData()

        //USING RETRYABLE
        val putDurationsUsingRetryable = getPutObjectDurations(data, false)
        printStats(putDurationsUsingRetryable)

        //USING TRANSFERMANAGER
        val putDurationsUsingTM = getPutObjectDurations(data, true)
        printStats(putDurationsUsingTM)

        val getDurations = getGetObjectDurations()
        printStats(getDurations)

        //clean up
        byteBlobDataManager.deleteObjects(keys)
    }

    private fun generateTestData(): ByteArray {
        val data = ByteArray(NUM_BYTES)
        Random().nextBytes(data)
        val numOfIterations = NUM_OBJECTS
        for (i in 1..numOfIterations) {
            var key = ""
            for (j in 1..3) {
                key = key.plus(UUID.randomUUID().toString()).plus("/")
            }
            key = key.plus(data.hashCode())
            keys.add(key)
        }
        return data
    }

    private fun getPutObjectDurations(data: ByteArray, useTM: Boolean): List<Long> {
        val durations = mutableListOf<Long>()
        var count = 0
        for (key in keys) {
            if (useTM) {
                StopWatch("Put", false).use {
                    byteBlobDataManager.putObjectWithTransferManager(key, data, "png")
                    durations.add(it.getDuration())
                }
            } else {
                StopWatch("Put", false).use {
                    byteBlobDataManager.putObject(key, data, "png")
                    durations.add(it.getDuration())
                }
                count++
                if (count % 10 == 0) {
                    logger.info("$count objects put")
                }
            }
        }
        return durations
    }

    private fun getGetObjectDurations(): List<Long> {
        var count = 0
        val durations = mutableListOf<Long>()
        for (key in keys) {
            StopWatch("Get", false).use {
                val url = byteBlobDataManager.getObjects(listOf(key))[0] as URL
                val connection = url.openConnection()
                val inputStream = connection.getInputStream()
                val data = inputStream.readAllBytes()
                durations.add(it.getDuration())
                inputStream.close()
            }
            count++
            if (count % 10 == 0) {
                logger.info("$count objects gotten")
            }
        }
        return durations
    }

    private fun printStats(durations: List<Long>) {
        val mean = durations.average()
        val max = durations.max()
        val summation = durations.map { (it - mean).pow(2.0) }.sum()
        val variance = summation / (durations.size - 1)
        val sd = sqrt(variance)
        logger.info("$NUM_OBJECTS objects of $NUM_BYTES bytes:\nmean = $mean\nmax = $max\nvariance = $variance\nstandard dev = $sd")
    }
}