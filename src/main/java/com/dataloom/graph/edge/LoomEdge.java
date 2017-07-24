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

package com.dataloom.graph.edge;

import java.util.UUID;

public class LoomEdge {
    private EdgeKey key;

    private UUID    srcType;
    private UUID srcSetId;
    private UUID dstSetId;
    private UUID edgeSetId;

    public LoomEdge( EdgeKey key, UUID srcType, UUID srcSetId, UUID dstSetId, UUID edgeSetId ) {
        this.key = key;
        this.srcType = srcType;
        this.srcSetId = srcSetId;
        this.dstSetId = dstSetId;
        this.edgeSetId = edgeSetId;
    }

    public EdgeKey getKey() {
        return key;
    }


    public UUID getSrcType() {
        return srcType;
    }

    public UUID getSrcSetId() {
        return srcSetId;
    }

    public UUID getDstSetId() {
        return dstSetId;
    }

    public UUID getEdgeSetId() {
        return edgeSetId;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ( ( dstSetId == null ) ? 0 : dstSetId.hashCode() );
        result = prime * result + ( ( edgeSetId == null ) ? 0 : edgeSetId.hashCode() );
        result = prime * result + ( ( key == null ) ? 0 : key.hashCode() );
        result = prime * result + ( ( srcSetId == null ) ? 0 : srcSetId.hashCode() );
        result = prime * result + ( ( srcType == null ) ? 0 : srcType.hashCode() );
        return result;
    }

    @Override
    public boolean equals( Object obj ) {
        if ( this == obj ) return true;
        if ( obj == null ) return false;
        if ( getClass() != obj.getClass() ) return false;
        LoomEdge other = (LoomEdge) obj;
        if ( dstSetId == null ) {
            if ( other.dstSetId != null ) return false;
        } else if ( !dstSetId.equals( other.dstSetId ) ) return false;
        if ( edgeSetId == null ) {
            if ( other.edgeSetId != null ) return false;
        } else if ( !edgeSetId.equals( other.edgeSetId ) ) return false;
        if ( key == null ) {
            if ( other.key != null ) return false;
        } else if ( !key.equals( other.key ) ) return false;
        if ( srcSetId == null ) {
            if ( other.srcSetId != null ) return false;
        } else if ( !srcSetId.equals( other.srcSetId ) ) return false;
        if ( srcType == null ) {
            if ( other.srcType != null ) return false;
        } else if ( !srcType.equals( other.srcType ) ) return false;
        return true;
    }
}