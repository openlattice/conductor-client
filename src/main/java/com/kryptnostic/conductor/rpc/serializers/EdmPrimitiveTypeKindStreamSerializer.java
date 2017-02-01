package com.kryptnostic.conductor.rpc.serializers;

import java.io.IOException;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.springframework.stereotype.Component;

import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;

@Component
public class EdmPrimitiveTypeKindStreamSerializer implements SelfRegisteringStreamSerializer<EdmPrimitiveTypeKind> {
    private static EdmPrimitiveTypeKind[] VALUES = EdmPrimitiveTypeKind.values();

    @Override
    public void write( ObjectDataOutput out, EdmPrimitiveTypeKind object ) throws IOException {
        out.writeInt( object.ordinal() );
    }

    @Override
    public EdmPrimitiveTypeKind read( ObjectDataInput in ) throws IOException {
        return VALUES[ in.readInt() ];
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.EDM_PRIMITIVE_TYPE_KIND.ordinal();
    }

    @Override
    public void destroy() {}

    @Override
    public Class<EdmPrimitiveTypeKind> getClazz() {
        return EdmPrimitiveTypeKind.class;
    }

}
