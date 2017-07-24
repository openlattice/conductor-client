/*
 * Copyright (C) 2017. OpenLattice, Inc
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
 */

package com.dataloom.data;

import com.datastax.driver.core.Row;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.kryptnostic.datastore.cassandra.RowAdapters;
import java.util.UUID;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public final class EntitySets {
    private EntitySets() {}

    public static Predicate filterByEntitySetIdAndSyncId( Row row  ){
        UUID entitySetId = RowAdapters.entitySetId(  row  );
        UUID syncId = RowAdapters.currentSyncId( row );
        return filterByEntitySetIdAndSyncId( entitySetId,syncId );
    }
    public static Predicate filterByEntitySetIdAndSyncId( UUID entitySetId, UUID syncId) {
        return Predicates.and(
                Predicates.equal( "entitySetId", entitySetId ),
                Predicates.equal( "syncId", syncId ) );
    }
}
