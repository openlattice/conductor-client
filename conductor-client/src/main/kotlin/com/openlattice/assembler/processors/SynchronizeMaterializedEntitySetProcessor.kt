/*
 * Copyright (C) 2019. OpenLattice, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@openlattice.com
 *
 *
 */

package com.openlattice.assembler.processors

import com.hazelcast.core.Offloadable
import com.hazelcast.spi.impl.executionservice.ExecutionService
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.assembler.AssemblerConnectionManager
import com.openlattice.assembler.AssemblerConnectionManagerDependent
import com.openlattice.assembler.EntitySetAssemblyKey
import com.openlattice.assembler.MaterializedEntitySet
import com.openlattice.authorization.Principal
import com.openlattice.edm.EntitySet
import com.openlattice.edm.type.PropertyType
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import java.time.OffsetDateTime
import java.util.*

@SuppressFBWarnings(value = ["SE_BAD_FIELD"], justification = "Custom Stream Serializer is implemented")
data class SynchronizeMaterializedEntitySetProcessor(
        val entitySet: EntitySet,
        val materializablePropertyTypes: Map<UUID, PropertyType>,
        val authorizedPropertyTypesOfPrincipals: Map<Principal, Set<PropertyType>>
) : AbstractRhizomeEntryProcessor<EntitySetAssemblyKey, MaterializedEntitySet, Void?>(),
        AssemblerConnectionManagerDependent<SynchronizeMaterializedEntitySetProcessor>,
        Offloadable {
    @Transient
    private var acm: AssemblerConnectionManager? = null

    override fun process(entry: MutableMap.MutableEntry<EntitySetAssemblyKey, MaterializedEntitySet?>): Void? {
        val organizationId = entry.key.organizationId
        val materializedEntitySet = entry.value
        if (materializedEntitySet == null) {
            throw IllegalStateException("Encountered null materialized entity set while trying to synchronize " +
                    "materialized view for entity set ${entitySet.id} in organization $organizationId.")
        } else {
            acm?.materializeEntitySets(
                    organizationId,
                    mapOf(entitySet to materializablePropertyTypes),
                    mapOf(entitySet.id to authorizedPropertyTypesOfPrincipals)
            ) ?: throw IllegalStateException(AssemblerConnectionManagerDependent.NOT_INITIALIZED)

            // Clear edm and data unsync flags.
            // Note: if we will have other flags, we only need to remove these 2 and not clear all!
            materializedEntitySet.flags.clear()
            // Update last refresh
            materializedEntitySet.lastRefresh = OffsetDateTime.now()

            entry.setValue(materializedEntitySet)
        }

        return null
    }

    override fun getExecutorName(): String {
        return ExecutionService.OFFLOADABLE_EXECUTOR
    }

    override fun init(acm: AssemblerConnectionManager): SynchronizeMaterializedEntitySetProcessor {
        this.acm = acm
        return this
    }

}