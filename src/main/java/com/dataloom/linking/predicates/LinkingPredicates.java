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

package com.dataloom.linking.predicates;

import com.dataloom.data.EntityKey;
import com.dataloom.linking.LinkingEdge;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;

import java.util.Set;
import java.util.UUID;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public final class LinkingPredicates {
    private LinkingPredicates() {
    }

    public static Predicate minimax( UUID graphId , double minimax ) {
        return Predicates.and( graphId( graphId ), Predicates.lessEqual( "this", minimax ) );
    }

    public static Predicate getAllEdges( LinkingEdge edge ) {
        return Predicates.and(
                Predicates.equal( "__key#graphId", edge.getGraphId() ),
                Predicates.or(
                        Predicates.in( "__key#srcId", edge.getSrcId(), edge.getDstId() ),
                        Predicates.in( "__key#dstId", edge.getSrcId(), edge.getDstId() ) )
        );
    }

    public static Predicate graphId( UUID graphId ) {
        return Predicates.equal( "__key#graphId", graphId );
    }

    public static Predicate linkingEntitiesFromEntityKeys( EntityKey[] entityKeys ) {
        return Predicates.in( "__key", entityKeys );
    }

    public static Predicate linkingEntityKeyIdPairsFromGraphId( UUID graphId ) {
        return Predicates.equal( "this", graphId );
    }

    public static Predicate between( double bottom, double top ) {
        return Predicates.between( "this", bottom, top );
    }

    public static Predicate entitiesFromKeysAndGraphId( EntityKey[] entityKeys, UUID graphId ) {
        return Predicates.and(
                Predicates.equal( "__key#graphId", graphId ),
                Predicates.in( "__key#entityKey", entityKeys )
        );
    }
}
