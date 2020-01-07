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

package com.openlattice.edm.processors

import com.hazelcast.aggregation.Aggregator
import com.openlattice.edm.EntitySet
import com.openlattice.edm.set.EntitySetFlag
import java.util.*

data class EntitySetsFlagFilteringAggregator @JvmOverloads constructor(
        val filteringFlags: Set<EntitySetFlag>,
        val filteredEntitySetIds: MutableMap<UUID, EntitySet> = mutableMapOf()
) : Aggregator<Map.Entry<UUID, EntitySet>, Map<UUID, EntitySet>>() {

    override fun accumulate(input: Map.Entry<UUID, EntitySet>) {
        if (input.value.flags.containsAll(filteringFlags)) {
            filteredEntitySetIds[input.key] = input.value
        }
    }

    override fun combine(aggregator: Aggregator<*, *>) {
        if (aggregator is EntitySetsFlagFilteringAggregator) {
            filteredEntitySetIds.putAll(aggregator.filteredEntitySetIds)
        }
    }

    override fun aggregate(): Map<UUID, EntitySet> {
        return filteredEntitySetIds
    }
}