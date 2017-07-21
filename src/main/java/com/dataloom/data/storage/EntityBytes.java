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

package com.dataloom.data.storage;

import com.dataloom.client.serialization.SerializationConstants;
import com.dataloom.data.EntityKey;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.SetMultimap;
import java.util.UUID;

public class EntityBytes {
    private EntityKey                 key;
    private SetMultimap<UUID, byte[]> details;

    public EntityBytes(
            EntityKey key,
            SetMultimap<UUID, byte[]> details ) {
        this.key = key;
        this.details = details;
    }

    public EntityKey getKey() {
        return key;
    }

    public SetMultimap<UUID, byte[]> getDetails() {
        return details;
    }

    @JsonIgnore
    public UUID getEntitySetId() {
        return key.getEntitySetId();
    }

    @JsonIgnore
    public String getEntityId() {
        return key.getEntityId();
    }

    @JsonIgnore
    public UUID getSyncId() {
        return key.getSyncId();
    }

    @Override public boolean equals( Object o ) {
        if ( this == o ) { return true; }
        if ( !( o instanceof EntityBytes ) ) { return false; }

        EntityBytes that = (EntityBytes) o;

        if ( key != null ? !key.equals( that.key ) : that.key != null ) { return false; }
        return details != null ? details.equals( that.details ) : that.details == null;
    }

    @Override public int hashCode() {
        int result = key != null ? key.hashCode() : 0;
        result = 31 * result + ( details != null ? details.hashCode() : 0 );
        return result;
    }
}
