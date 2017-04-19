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

/**
 * An EdgeKey is the pojo for the primary key of edges table. In the current setting, this is source vertexId,
 * destination vertexId, and the entity key referencing the edge in the edge entity set.
 * 
 * @author Ho Chung Siu
 *
 */
public class EdgeKey {
    private final UUID srcEntityKeyId;
    private final UUID dstEntityKeyId;
    private final UUID dstTypeId;
    private final UUID edgeTypeId;
    private final UUID edgeEntityKeyId;

    public EdgeKey( UUID srcEntityKeyId, UUID dstEntityKeyId, UUID dstTypeId, UUID edgeTypeId, UUID edgeEntityKeyId ) {
        this.srcEntityKeyId = srcEntityKeyId;
        this.dstTypeId = dstTypeId;
        this.edgeTypeId = edgeTypeId;
        this.dstEntityKeyId = dstEntityKeyId;
        this.edgeEntityKeyId = edgeEntityKeyId;
    }

    public UUID getSrcEntityKeyId() {
        return srcEntityKeyId;
    }

    public UUID getDstEntityKeyId() {
        return dstEntityKeyId;
    }

    public UUID getDstTypeId() {
        return dstTypeId;
    }

    public UUID getEdgeTypeId() {
        return edgeTypeId;
    }

    public UUID getEdgeEntityKeyId() {
        return edgeEntityKeyId;
    }

}
