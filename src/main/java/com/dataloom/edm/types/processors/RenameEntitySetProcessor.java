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

package com.dataloom.edm.types.processors;

import java.util.Map.Entry;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import java.util.UUID;

import com.dataloom.edm.internal.EntitySet;
import com.google.common.base.Optional;
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor;

public class RenameEntitySetProcessor extends AbstractRhizomeEntryProcessor<UUID, EntitySet, Object> {
    private static final long serialVersionUID = 2745124176656423898L;
    private final String      newName;

    public RenameEntitySetProcessor( String newName ) {
        this.newName = newName;
    }

    @Override
    public Object process( Entry<UUID, EntitySet> entry ) {
        EntitySet es = entry.getValue();
        if ( es != null ) {
            EntitySet newEs = new EntitySet(
                    es.getId(),
                    es.getEntityTypeId(),
                    newName,
                    es.getTitle(),
                    Optional.of( es.getDescription() ) );
            entry.setValue( newEs );
        }
        return null;
    }
    
    public String getName(){
        return newName;
    }

}