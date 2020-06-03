package com.openlattice.admin

import com.geekbeast.rhizome.hazelcast.mockHazelcastMap
import com.geekbeast.rhizome.hazelcast.mockHazelcastQueue
import com.hazelcast.core.HazelcastInstance
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.hazelcast.HazelcastUtils
import org.junit.Assert
import org.junit.Test
import org.mockito.Matchers.any
import org.mockito.Mockito
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.TimeoutException

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class BridgeServiceTest {
    companion object {
        val hazelcastInstance = Mockito.mock(HazelcastInstance::class.java)
        val services = mockHazelcastMap(UUID::class.java, ServiceDescription::class.java)
        val operations = mockHazelcastMap(UUID::class.java, InvocationRequest::class.java)
        val results = mockHazelcastMap(InvocationResultKey::class.java, Any::class.java)

        val operationsQueue = mockHazelcastQueue(UUID::class.java)
        val resultsQueue = mockHazelcastQueue(InvocationResultKey::class.java)

        val bridgeAwareServices = BridgeAwareServices()
        val bridgeService: BridgeService
        val executor = Executors.newFixedThreadPool(2)

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
                            operationsQueue
                        } else {
                            resultsQueue
                        }
                    }
            bridgeService = BridgeService(
                    ServiceDescription(ServiceType.REHEARSAL, mutableListOf(), mutableMapOf()),
                    bridgeAwareServices,
                    hazelcastInstance
            )
        }


    }

    @Test
    fun testAwaitCluster() {
        val desiredCluster = mapOf(ServiceType.CONDUCTOR to 3, ServiceType.DATASTORE to 2)
        val futureCluster = executor.submit<Map<ServiceType, Map<UUID, ServiceDescription>>> {
            return@submit bridgeService.awaitCluster(
                    desiredCluster
            )
        }
        registerService(ServiceType.CONDUCTOR)
        registerService(ServiceType.CONDUCTOR)
        registerService(ServiceType.CONDUCTOR)
        Assert.assertFalse("Desired cluster should not be complete", futureCluster.isDone)
        registerService(ServiceType.DATASTORE)
        registerService(ServiceType.DATASTORE)


        val cluster = futureCluster.get()

        Assert.assertTrue("Desired cluster should be complete", futureCluster.isDone)
        Assert.assertEquals( 3, cluster.getValue( ServiceType.CONDUCTOR).size )
        Assert.assertEquals( 3, cluster.getValue( ServiceType.DATASTORE).size )
    }

    @Test(expected = TimeoutException::class)
    fun testAwaitClusterTimeout() {
        val desiredCluster = mapOf(ServiceType.CONDUCTOR to 3, ServiceType.DATASTORE to 2)
        val cluster = bridgeService.awaitCluster(desiredCluster, 250)
    }

    private fun registerService(serviceType: ServiceType) = HazelcastUtils.insertIntoUnusedKey(
            services,
            ServiceDescription(serviceType),
            UUID::randomUUID,
            300
    )
}


