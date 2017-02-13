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

package com.dataloom.hazelcast.serializers;

import java.io.IOException;

import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.springframework.stereotype.Component;

import com.dataloom.edm.types.processors.RenameEntityTypeProcessor;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;

@Component
public class RenameEntityTypeProcessorStreamSerializer
        implements SelfRegisteringStreamSerializer<RenameEntityTypeProcessor> {

    @Override
    public void write( ObjectDataOutput out, RenameEntityTypeProcessor object ) throws IOException {
        FullQualifiedNameStreamSerializer.serialize( out, object.getFullQualifiedName() );
    }

    @Override
    public RenameEntityTypeProcessor read( ObjectDataInput in ) throws IOException {
        FullQualifiedName newFqn = FullQualifiedNameStreamSerializer.deserialize( in );
        return new RenameEntityTypeProcessor( newFqn );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.RENAME_ENTITY_TYPE_PROCESSOR.ordinal();
    }

    @Override
    public void destroy() {}

    @Override
    public Class<RenameEntityTypeProcessor> getClazz() {
        return RenameEntityTypeProcessor.class;
    }
}
