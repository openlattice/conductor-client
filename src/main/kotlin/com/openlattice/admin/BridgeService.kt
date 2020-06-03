package com.openlattice.admin

import com.geekbeast.rhizome.services.ServiceState
import com.google.common.eventbus.Subscribe
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.core.IQueue
import com.hazelcast.query.Predicate
import com.hazelcast.query.Predicates
import com.openlattice.data.storage.StorageManagementService
import com.openlattice.data.storage.StorageType
import com.openlattice.datastore.services.EdmManager
import com.openlattice.datastore.services.EntitySetService
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.hazelcast.HazelcastUtils
import org.apache.commons.lang3.NotImplementedException
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import org.springframework.stereotype.Service
import java.lang.IllegalStateException
import java.util.*
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

const val OPERATION_QUEUES_PREFIX = "operations_"
const val RESULT_QUEUES_PREFIX = "results_"

/**
 * Enables clusterwide application of restartable operations.
 *
 * Restartable means that an operation can be restarted after a failure without negative side-effects.
 *
 * This service is only safe to use for restartable operations. That is if the service or cluster fails
 * during setup or invocation the calling service should be able to safely resubmit an identical request without risk
 * of negative side effects.
 *
 * It is recommended the operations requiring transactions be managed at a higher layer and use this as a building block.
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Service
class BridgeService(
        val serviceDescription: ServiceDescription,
        val bridgeAwareServices: BridgeAwareServices,
        private val hazelcastInstance: HazelcastInstance
) {
    companion object {
        private val logger = LoggerFactory.getLogger(BridgeService::class.java)
    }

    val serviceId = register(serviceDescription)

    private var started = false

    private val services = HazelcastMap.SERVICES.getMap(hazelcastInstance)
    private val operations = HazelcastMap.OPERATIONS.getMap(hazelcastInstance)
    private val results = HazelcastMap.RESULTS.getMap(hazelcastInstance)

    private val operationQueueName = buildOperationQueueName(serviceId)
    private val resultQueueName = buildResultQueueName(serviceId)

    private val operationQueue = hazelcastInstance.getQueue<UUID>(operationQueueName)
    private val resultQueue = hazelcastInstance.getQueue<InvocationResultKey>(resultQueueName)

    private val operationQueues = mutableMapOf<UUID, IQueue<UUID>>() //service id -> operations queue
    private val resultQueues = mutableMapOf<UUID, IQueue<UUID>>() //service id -> results queue
    private val resultLocks = mutableMapOf<UUID, CountDownLatch>() //(service id ) -> count down latch
    private val resultResponses = mutableMapOf<UUID, MutableSet<UUID>>() // (operation id ) -> set of service ids

    private final val pingingExecutor = Executors.newSingleThreadExecutor()
    private final val operationExecutor = Executors.newSingleThreadExecutor()
    private final val resultsExecutor = Executors.newSingleThreadExecutor()


    val pinger = pingingExecutor.execute {
        while (true) {
            ping()
            Thread.sleep(4 * 60 * 1000)
        }
    }

    val worker = operationExecutor.execute {
        while (true) {
            val operationId = operationQueue.take()
            val invokationRequest = operations.getValue(operationId)
            val invocationResult = InvocationResultKey(serviceId, operationId)
            results[invocationResult] = invokationRequest.operation(bridgeAwareServices)

        }
    }

    val resultsWorker = resultsExecutor.execute {
        while (true) {
            val invocationResultKey = resultQueue.take()
            //Record that other service responded.
            resultResponses
                    .getOrPut(invocationResultKey.operationId) { mutableSetOf() }
                    .add(invocationResultKey.responder)

            //Record that invocation happened
            resultLocks.getValue(invocationResultKey.operationId).countDown()
        }
    }

    /**
     * Assigns and registers the current service with a unique id.
     */
    fun register(service: ServiceDescription): UUID = HazelcastUtils.insertIntoUnusedKey(
            services,
            service,
            UUID::randomUUID,
            300
    )

    fun operateOnAllServices(operation: (BridgeAwareServices) -> Any?) {
        throw NotImplementedException("Blame MTR. Wait, what madness brought you here?")
    }

    fun operatedOnTaggedServices(
            tags: List<String>,
            serviceType: ServiceType,
            timeout: Long = 0,
            timeoutUnit: TimeUnit = TimeUnit.MILLISECONDS,
            operation: (BridgeAwareServices) -> Any?
    ): Map<InvocationResultKey, Any?> {
        val tagsFilter = Predicates.`in`("tags[any]", *tags.toTypedArray())
        val serviceTypeFilter = getServiceTypePredicate(serviceType)
        val serviceIds = services.keySet(Predicates.and(tagsFilter, serviceTypeFilter))

        return invoke(serviceIds, operation, timeout, timeoutUnit)
    }

    fun operateOnServicesOfType(
            serviceType: ServiceType,
            timeout: Long = 0,
            timeoutUnit: TimeUnit = TimeUnit.MILLISECONDS,
            operation: (BridgeAwareServices) -> Any?
    ): Map<InvocationResultKey, Any?> {
        val serviceTypeFilter = getServiceTypePredicate(serviceType)
        val serviceIds = services.keySet(serviceTypeFilter)
        return invoke(serviceIds, operation, timeout, timeoutUnit)
    }

    fun isStarted(): Boolean {
        return started
    }

    @Subscribe
    fun notifyOfStateChange(serviceState: ServiceState) {
        started = (serviceState == ServiceState.RUNNING)
    }

    private fun invoke(
            serviceIds: Set<UUID>,
            operation: (BridgeAwareServices) -> Any?,
            timeout: Long = 0,
            timeoutUnit: TimeUnit = TimeUnit.MILLISECONDS
    ): Map<InvocationResultKey, Any?> {
        val invocationRequest = InvocationRequest(serviceId, operation)
        val operationId = HazelcastUtils.insertIntoUnusedKey(operations, invocationRequest, UUID::randomUUID, 300)
        val invocationResultKeys = serviceIds.map { InvocationResultKey(it, operationId) }.toSet()

        //Acquire completion locks before submission.
        val invocationLatch = CountDownLatch(invocationResultKeys.size)

        try {
            //Submit actual invocation to nodes
            serviceIds.forEach { serviceId ->
                operationQueues
                        .getOrPut(serviceId) { hazelcastInstance.getQueue(buildOperationQueueName(serviceId)) }
                        .put(operationId)
            }

            if (timeout > 0) {
                invocationLatch.await(timeout, timeoutUnit)
            } else {
                invocationLatch.await()
            }
        } catch (ex: Exception) {
            logger.error("Operation $operationId failed. ", ex)
            throw IllegalStateException("Operation $operationId failed.", ex)
        }

        //Retrieve the results and clear the maps.
        try {
            val invocationResults = results.getAll(invocationResultKeys) as MutableMap<InvocationResultKey, Any?>
            (invocationResultKeys - invocationResults.keys).forEach { invocationResults[it] = null }
            return invocationResults
        } finally {
            operations.delete(operationId)
            results.delete(operationId)
        }
    }


    fun getServiceTypePredicate(serviceType: ServiceType): Predicate<*, *> = Predicates.equal(
            "serviceType",
            serviceType.name
    )

    fun getServiceTypesPredicate(vararg serviceTypes: ServiceType): Predicate<*, *> = Predicates.`in`(
            "serviceType",
            *serviceTypes.map { it.name }.toTypedArray()
    )

    /**
     * Waits until the required services have been registered in Hazelcast.
     * @param desiredCluster The desired number of services of each type.
     * @param timeout An optional amount of time to wait for the cluster to reach this state
     * @param timeunit The time unit for the [timeout] parameter.
     */
    fun awaitCluster(
            desiredCluster: Map<ServiceType, Int>,
            timeout: Long = 0,
            timeunit: TimeUnit = TimeUnit.MILLISECONDS
    ): Map<ServiceType, Map<UUID, ServiceDescription>> {
        val start = System.nanoTime()
        var currentCluster: Map<ServiceType, Map<UUID, ServiceDescription>>
        var desiredClusterStateReached: Boolean
        do {
            currentCluster =
                    services
                            .entrySet(getServiceTypesPredicate(*desiredCluster.keys.toTypedArray()))
                            .groupBy({ it.value.serviceType }, { it.key to it.value })
                            .mapValues { it.value.toMap() }
            desiredClusterStateReached =
                    desiredCluster.all { (serviceType, count) -> (currentCluster[serviceType] ?: 0) == count }
            Thread.sleep(100)
        } while (!desiredClusterStateReached &&
                (timeout == 0L || ((System.nanoTime() - start) < timeunit.toNanos(timeout))))

        if (!desiredClusterStateReached) {
            throw TimeoutException("Unable to reach desired cluster state in $timeout $timeunit")
        }

        return currentCluster
    }

    fun ping() {
        //Being lazy, should do this via an entry processor.
        serviceDescription.lastPing = System.currentTimeMillis()
        services.set(serviceId, serviceDescription)
    }

}

private fun buildOperationQueueName(serviceId: UUID) = "$OPERATION_QUEUES_PREFIX${serviceId.toString().replace(
        "-",
        ""
)}"

private fun buildResultQueueName(serviceId: UUID) = "$RESULT_QUEUES_PREFIX${serviceId.toString().replace("-", "")}"

data class ServiceDescription @JvmOverloads constructor(
        val serviceType: ServiceType,
        val tags: MutableList<String> = mutableListOf(),
        val operations: Map<UUID, InvocationRequest> = mutableMapOf(),
        var lastPing: Long = System.currentTimeMillis()
)

@Component
class BridgeAwareServices {
    @Autowired(required = false)
    lateinit var entitySetService: EntitySetService

    @Autowired(required = false)
    lateinit var edmService: EdmManager

    @Autowired(required = false)
    lateinit var storageManagementService: StorageManagementService
}

data class InvocationResultKey(val responder: UUID, val operationId: UUID)
class InvocationRequest(val invoker: UUID, val operation: (BridgeAwareServices) -> Any?)
