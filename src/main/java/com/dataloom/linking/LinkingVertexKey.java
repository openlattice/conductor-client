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

package com.dataloom.linking;

import java.util.UUID;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public class LinkingVertexKey implements Comparable<LinkingVertexKey> {
    private final UUID graphId;
    private final UUID vertexId;

    public LinkingVertexKey( UUID graphId, UUID vertexId ) {
        this.graphId = graphId;
        this.vertexId = vertexId;
    }

    public UUID getGraphId() {
        return graphId;
    }

    public UUID getVertexId() {
        return vertexId;
    }

    @Override
    public int compareTo( LinkingVertexKey o ) {
        checkArgument( graphId.equals( o.getGraphId() ), "Cannot compare vertices with different graph ids." );
        return vertexId.compareTo( o.graphId );
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ( ( graphId == null ) ? 0 : graphId.hashCode() );
        result = prime * result + ( ( vertexId == null ) ? 0 : vertexId.hashCode() );
        return result;
    }

    @Override
    public boolean equals( Object obj ) {
        if ( this == obj ) {
            return true;
        }
        if ( obj == null ) {
            return false;
        }
        if ( !( obj instanceof LinkingVertexKey ) ) {
            return false;
        }
        LinkingVertexKey other = (LinkingVertexKey) obj;
        if ( graphId == null ) {
            if ( other.graphId != null ) {
                return false;
            }
        } else if ( !graphId.equals( other.graphId ) ) {
            return false;
        }
        if ( vertexId == null ) {
            if ( other.vertexId != null ) {
                return false;
            }
        } else if ( !vertexId.equals( other.vertexId ) ) {
            return false;
        }
        return true;
    }

    @Override public String toString() {
        return "LinkingVertexKey{" +
                "graphId=" + graphId +
                ", vertexId=" + vertexId +
                '}';
    }
}
