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

import com.google.common.base.Preconditions;

import java.util.UUID;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public class LinkingEdge {
    private final LinkingVertexKey src;
    private final LinkingVertexKey dst;

    public LinkingEdge( LinkingVertexKey src, LinkingVertexKey dst ) {
        Preconditions.checkArgument( src.getGraphId().equals( dst.getGraphId() ) );
        if ( src.compareTo( dst ) < 0 ) {
            this.src = src;
            this.dst = dst;
        } else {
            this.src = dst;
            this.dst = src;
        }
    }

    public UUID getGraphId() {
        return src.getGraphId();
    }

    public LinkingVertexKey getSrc() {
        return src;
    }

    public LinkingVertexKey getDst() {
        return dst;
    }

    @Override public String toString() {
        return "LinkingEdge{" +
                "src=" + src +
                ", dst=" + dst +
                '}';
    }
}
