package com.openlattice.materializer

import com.openlattice.tasks.HazelcastFixedRateTask
import com.openlattice.tasks.Task
import java.util.concurrent.TimeUnit

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class ComputeDataDeltasTask : HazelcastFixedRateTask<ComputeDataDeltasDependencies> {
    override fun getInitialDelay(): Long {
        return 0
    }

    override fun getPeriod(): Long {
        return 100
    }

    override fun getTimeUnit(): TimeUnit {
        return TimeUnit.MILLISECONDS
    }

    override fun runTask() {

    }

    override fun getName(): String {
        return Task.COMPUTE_DATA_DELTAS.name
    }

    override fun getDependenciesClass(): Class<out ComputeDataDeltasDependencies> {
        return ComputeDataDeltasDependencies::class.java
    }
}