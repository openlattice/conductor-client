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

import org.springframework.stereotype.Component;

import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.dataloom.requests.processors.UpdateRequestStatusProcessor;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;

@Component
public class UpdateRequestStatusProcessorStreamSerializer
        implements SelfRegisteringStreamSerializer<UpdateRequestStatusProcessor> {

    @Override
    public void write( ObjectDataOutput out, UpdateRequestStatusProcessor object ) throws IOException {
        RequestStatusStreamSerializer.serialize( out, object.getRequestStatus() );
    }

    @Override
    public UpdateRequestStatusProcessor read( ObjectDataInput in ) throws IOException {
        return new UpdateRequestStatusProcessor( RequestStatusStreamSerializer.deserialize( in ) );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.UPDATE_REQUEST_STATUS_PROCESSOR.ordinal();
    }

    @Override
    public void destroy() {}

    @Override
    public Class<UpdateRequestStatusProcessor> getClazz() {
        return UpdateRequestStatusProcessor.class;
    }

}
