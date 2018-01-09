/*
 * Copyright (C) 2018. OpenLattice, Inc
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

package com.openlattice.data;

import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * Metadata associated with an entity that tracks last write time, last index time, and current version.
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class EntityDataMetadata {
    private final long           version;
    private final OffsetDateTime lastWrite;
    private final OffsetDateTime lastIndex;

    public EntityDataMetadata( long version, OffsetDateTime lastWrite, OffsetDateTime lastIndex ) {
        this.version = version;
        this.lastWrite = lastWrite;
        this.lastIndex = lastIndex;
    }

    public long getVersion() {
        return version;
    }

    public OffsetDateTime getLastWrite() {
        return lastWrite;
    }

    public OffsetDateTime getLastIndex() {
        return lastIndex;
    }
}
