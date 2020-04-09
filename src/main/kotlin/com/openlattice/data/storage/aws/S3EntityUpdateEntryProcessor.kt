package com.openlattice.data.storage.aws

import com.hazelcast.core.Offloadable
import com.hazelcast.core.Offloadable.OFFLOADABLE_EXECUTOR
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.data.Entity
import com.openlattice.data.EntityDataKey
import com.openlattice.data.UpdateType
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
data class S3EntityUpdateEntryProcessor(
        val update: Map<UUID, Set<Any>>,
        val updateType: UpdateType
) : AbstractRhizomeEntryProcessor<EntityDataKey, Entity, Void?>(), Offloadable {

    override fun getExecutorName(): String = OFFLOADABLE_EXECUTOR

    override fun process(entry: MutableMap.MutableEntry<EntityDataKey, Entity>): Void? {
        val entity = entry.value

        when (updateType) {
            UpdateType.Merge -> {
                update.forEach { (propertyTypeId, values) ->
                    entity
                            .getOrPut(propertyTypeId) { mutableSetOf() }
                            .addAll(values)

                }
            }
            UpdateType.PartialReplace -> {
                update.forEach { (propertyTypeId, values) ->
                    entity[propertyTypeId] = values.toMutableSet()
                }
            }
            UpdateType.Replace -> {
                entity.clear()
                update.forEach { (proeprtyTypeId, values) ->
                    entity[proeprtyTypeId] = values.toMutableSet()
                }
            }
        }

        entry.setValue(entity)
        return null
    }

}

