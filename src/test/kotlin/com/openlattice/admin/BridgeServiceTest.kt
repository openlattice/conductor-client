package com.openlattice.admin

import com.geekbeast.rhizome.hazelcast.mockHazelcastMap
import com.geekbeast.rhizome.hazelcast.mockHazelcastQueue
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.core.IQueue
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.hazelcast.HazelcastUtils
import org.junit.Assert
import org.junit.Before
import org.junit.Test
import org.mockito.Matchers.any
import org.mockito.Mockito
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.TimeoutException

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class BridgeServiceTest {
    companion object {
        private val logger = LoggerFactory.getLogger(BridgeServiceTest::class.java)
        private val hazelcastInstance = Mockito.mock(HazelcastInstance::class.java)

        private val services = mockHazelcastMap(UUID::class.java, ServiceDescription::class.java)
        private val operations = mockHazelcastMap(UUID::class.java, InvocationRequest::class.java)
        private val results = mockHazelcastMap(InvocationResultKey::class.java, Any::class.java)

        private val operationsQueues = mutableMapOf<String, IQueue<UUID>>()
        private val resultsQueues = mutableMapOf<String, IQueue<InvocationResultKey>>()

        private var bridgeAwareServices = BridgeAwareServices()
        private lateinit var bridgeService: BridgeService
        private val executor = Executors.newFixedThreadPool(2)

        init {
            Mockito.`when`(hazelcastInstance.getMap<UUID, ServiceDescription>(HazelcastMap.SERVICES.name))
                    .thenReturn(services)
            Mockito.`when`(hazelcastInstance.getMap<UUID, InvocationRequest>(HazelcastMap.OPERATIONS.name))
                    .thenReturn(operations)
            Mockito.`when`(hazelcastInstance.getMap<InvocationResultKey, Any>(HazelcastMap.RESULTS.name))
                    .thenReturn(results)
            Mockito.`when`(hazelcastInstance.getQueue<Any>(any(String::class.java)))
                    .thenAnswer {
                        val name = it.arguments[0] as String
                        if (name.startsWith(OPERATION_QUEUES_PREFIX)) {
                            operationsQueues.getOrPut(name) { mockHazelcastQueue(UUID::class.java) }
                        } else {
                            resultsQueues.getOrPut(name) { mockHazelcastQueue(InvocationResultKey::class.java) }
                        }
                    }
        }

        private fun initializeRehearsalBridgeService() {
            bridgeService = initializeBridgeService(ServiceType.REHEARSAL, bridgeAwareServices)
        }

        private fun clearMaps() {
            services.clear()
            operations.clear()
            results.clear()
        }

        private fun clearQueues() {
            operationsQueues.clear()
            resultsQueues.clear()
        }

        private fun initializeBridgeService(
                serviceType: ServiceType,
                bridgeAwareServices: BridgeAwareServices = BridgeAwareServices(),
                tags: MutableList<String> = mutableListOf()
        ): BridgeService = BridgeService(
                ServiceDescription(serviceType, tags),
                bridgeAwareServices,
                hazelcastInstance
        )

    }

    @Before
    fun clearState() {
        clearMaps()
        clearQueues()
        initializeRehearsalBridgeService()
    }

    @Test
    fun testAwaitCluster() {
        val desiredCluster = mapOf(ServiceType.CONDUCTOR to 3, ServiceType.DATASTORE to 2)
        val futureCluster = executor.submit<Map<ServiceType, Map<UUID, ServiceDescription>>> {
            return@submit bridgeService.awaitCluster(
                    desiredCluster
            )
        }
        initializeBridgeService(ServiceType.CONDUCTOR)
        initializeBridgeService(ServiceType.CONDUCTOR)
        initializeBridgeService(ServiceType.CONDUCTOR)
        Assert.assertFalse("Desired cluster should not be complete", futureCluster.isDone)
        initializeBridgeService(ServiceType.DATASTORE)
        initializeBridgeService(ServiceType.DATASTORE)


        val cluster = futureCluster.get()

        Assert.assertTrue("Desired cluster should be complete", futureCluster.isDone)
        Assert.assertEquals(3, cluster.getValue(ServiceType.CONDUCTOR).size)
        Assert.assertEquals(2, cluster.getValue(ServiceType.DATASTORE).size)
    }

    @Test
    fun testInvocationOnAllService() {
        logger.info("Testing invocation on all services.")
        val desiredCluster = mapOf(ServiceType.CONDUCTOR to 3, ServiceType.DATASTORE to 2)

        val expected = setOf(
                bridgeService.serviceId,
                initializeBridgeService(ServiceType.CONDUCTOR).serviceId,
                initializeBridgeService(ServiceType.CONDUCTOR).serviceId,
                initializeBridgeService(ServiceType.CONDUCTOR).serviceId,
                initializeBridgeService(ServiceType.DATASTORE).serviceId,
                initializeBridgeService(ServiceType.DATASTORE).serviceId
        )

        logger.info("Cluster state: {}", bridgeService.awaitCluster(desiredCluster))

        Assert.assertEquals(
                expected,
                bridgeService.operateOnAllServices { it.bridgeService.serviceId }.values.toSet()
        )

    }

    @Test(expected = TimeoutException::class)
    fun testAwaitClusterTimeout() {
        val desiredCluster = mapOf(ServiceType.CONDUCTOR to 3, ServiceType.DATASTORE to 2)
        val cluster = bridgeService.awaitCluster(desiredCluster, 250)
    }

    @Test
    fun testInvocationOnServiceByType() {
        logger.info("Testing invocation on all services.")
        val desiredCluster = mapOf(ServiceType.CONDUCTOR to 3, ServiceType.DATASTORE to 2)

        initializeBridgeService(ServiceType.CONDUCTOR)
        initializeBridgeService(ServiceType.CONDUCTOR)
        initializeBridgeService(ServiceType.CONDUCTOR)

        val expected = setOf(
                initializeBridgeService(ServiceType.DATASTORE).serviceId,
                initializeBridgeService(ServiceType.DATASTORE).serviceId
        )

        logger.info("Cluster state: {}", bridgeService.awaitCluster(desiredCluster))

        Assert.assertEquals(
                expected,
                bridgeService.operateOnServicesOfType(ServiceType.DATASTORE) { it.bridgeService.serviceId }.values.toSet()
        )
    }

    @Test
    fun testInvocationOnServiceByTypeAndTag() {
        logger.info("Testing invocation on all services.")
        val desiredCluster = mapOf(ServiceType.CONDUCTOR to 3, ServiceType.DATASTORE to 2)

        initializeBridgeService(ServiceType.CONDUCTOR)
        initializeBridgeService(ServiceType.CONDUCTOR)
        initializeBridgeService(ServiceType.DATASTORE)
        initializeBridgeService(ServiceType.CONDUCTOR, tags = mutableListOf("a"))

        val expected = setOf(
                initializeBridgeService(ServiceType.DATASTORE, tags = mutableListOf("a")).serviceId
        )

        logger.info("Cluster state: {}", bridgeService.awaitCluster(desiredCluster))

        Assert.assertEquals(
                expected,
                bridgeService.operatedOnTaggedServices(listOf("a"), ServiceType.DATASTORE) {
                    it.bridgeService.serviceId
                }.values.toSet()
        )
    }
}


