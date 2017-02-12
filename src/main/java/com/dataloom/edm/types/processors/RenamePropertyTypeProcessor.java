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
import java.util.UUID;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.dataloom.edm.PropertyType;
import com.google.common.base.Optional;
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor;

public class RenamePropertyTypeProcessor extends AbstractRhizomeEntryProcessor<UUID, PropertyType, Object> {
    private static final long       serialVersionUID = -7292989266784337619L;
    private final FullQualifiedName newFqn;

    public RenamePropertyTypeProcessor( FullQualifiedName newFqn ) {
        this.newFqn = newFqn;
    }

    @Override
    public Object process( Entry<UUID, PropertyType> entry ) {
        PropertyType pt = entry.getValue();
        if ( pt != null ) {
            PropertyType newPt = new PropertyType(
                    pt.getId(),
                    newFqn,
                    pt.getTitle(),
                    Optional.of( pt.getDescription() ),
                    pt.getSchemas(),
                    pt.getDatatype() );
            entry.setValue( newPt );
        }
        return null;
    }
    
    public FullQualifiedName getFullQualifiedName(){
        return newFqn;
    }

}