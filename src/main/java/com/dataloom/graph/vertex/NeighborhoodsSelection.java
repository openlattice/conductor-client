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

package com.dataloom.graph.vertex;

import java.util.Set;
import java.util.UUID;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public class NeighborhoodsSelection {
    private final Set<UUID> srcVertexIds;
    private final Set<UUID> edgeEntityTypeIds;
    private final Set<UUID> dstEntityTypeIds;

    public NeighborhoodsSelection(
            Set<UUID> srcVertexIds,
            Set<UUID> edgeEntityTypeIds,
            Set<UUID> dstEntityTypeIds ) {
        this.srcVertexIds = srcVertexIds;
        this.edgeEntityTypeIds = edgeEntityTypeIds;
        this.dstEntityTypeIds = dstEntityTypeIds;
    }

    public Set<UUID> getSrcVertexId() {
        return srcVertexIds;
    }

    public Set<UUID> getEdgeEntityTypeIds() {
        return edgeEntityTypeIds;
    }

    public Set<UUID> getDstEntityTypeIds() {
        return dstEntityTypeIds;
    }
}
