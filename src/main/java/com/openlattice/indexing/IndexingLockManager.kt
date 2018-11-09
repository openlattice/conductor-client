/*
 * Copyright (C) 2018. OpenLattice, Inc.
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

package com.openlattice.indexing

import com.hazelcast.core.HazelcastInstance
import com.hazelcast.core.IMap
import com.hazelcast.query.Predicate
import com.hazelcast.query.Predicates
import com.hazelcast.query.QueryConstants
import com.openlattice.hazelcast.HazelcastMap
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service
import java.util.*


private const val EXPIRATION_MILLIS = 60000L

/**
 * Manages locks for indexing.
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Service
class IndexingLockManager(hazelcastInstance: HazelcastInstance) {
    private val indexingLocks: IMap<UUID, Long> = hazelcastInstance.getMap(HazelcastMap.INDEXING_LOCKS.name)

    init {
        indexingLocks.addIndex(QueryConstants.THIS_ATTRIBUTE_NAME.value(), true)
    }

    @Scheduled(fixedRate = EXPIRATION_MILLIS)
    fun scavengeIndexingLocks() {
        indexingLocks.removeAll(
                Predicates.lessThan(
                        QueryConstants.THIS_ATTRIBUTE_NAME.value(),
                        System.currentTimeMillis()
                ) as Predicate<UUID, Long>
        )
    }

    fun unlock(entitySetId: UUID) {
        indexingLocks.delete(entitySetId)
    }

    fun tryLockEntitySet(entitySetId: UUID): Boolean {
        return indexingLocks.putIfAbsent(entitySetId, System.currentTimeMillis() + EXPIRATION_MILLIS) == null
    }

    fun updateExpiration(entitySetId: UUID) {
        indexingLocks.set(entitySetId, System.currentTimeMillis() + EXPIRATION_MILLIS)
    }
}