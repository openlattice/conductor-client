/*
 * Copyright (C) 2017. Kryptnostic, Inc (dba Loom)
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
 * You can contact the owner of the copyright at support@thedataloom.com
 */

package com.dataloom.graph;

import com.dataloom.data.EntityKey;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public final class GraphUtil {
    private static final Logger logger = LoggerFactory.getLogger( GraphUtil.class );

    private GraphUtil() {
    }

    public static EntityKey min( EntityKey a, EntityKey b ) {
        return a.compareTo( b ) < 0 ? a : b;
    }

    public static EntityKey max( EntityKey a, EntityKey b ) {
        return a.compareTo( b ) > 0 ? a : b;
    }

    public static LinkingEdge linkingEdge( UUID graphId, EntityKey a, EntityKey b ) {
        return new LinkingEdge( graphId, a, b );
    }

    public static DirectedEdge edge( EntityKey a, EntityKey b ) {
        return new DirectedEdge( a, b );
    }

    public static EntityKey source( Row rs ) {
        final UUID entitySetId = rs.getUUID( CommonColumns.SOURCE_ENTITY_SET_ID.cql() );
        final String entityId = rs.getString( CommonColumns.SOURCE_ENTITY_ID.cql() );
        return new EntityKey( entitySetId, entityId );
    }

    public static EntityKey destination( Row row ) {
        final UUID entitySetId = row.getUUID( CommonColumns.DESTINATION_ENTITY_SET_ID.cql() );
        final String entityId = row.getString( CommonColumns.DESTINATION_ENTITY_ID.cql() );
        return new EntityKey( entitySetId, entityId );
    }

    public static LinkingEdge linkingEdge( Row row ) {
        final UUID graphId = row.getUUID( CommonColumns.GRAPH_ID.cql() );
        final EntityKey src = source( row );
        final EntityKey dst = destination( row );

        return new LinkingEdge( graphId, src, dst );
    }

    public static Double edgeValue( Row row ) {
        return row.getDouble( CommonColumns.EDGE_VALUE.cql() );
    }
}
