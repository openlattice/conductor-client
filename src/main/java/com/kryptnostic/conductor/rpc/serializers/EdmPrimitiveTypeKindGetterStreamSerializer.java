package com.kryptnostic.conductor.rpc.serializers;

import java.io.IOException;

import org.springframework.stereotype.Component;

import com.dataloom.edm.processors.EdmPrimitiveTypeKindGetter;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;

@Component
public class EdmPrimitiveTypeKindGetterStreamSerializer
        implements SelfRegisteringStreamSerializer<EdmPrimitiveTypeKindGetter> {

    @Override
    public void write( ObjectDataOutput out, EdmPrimitiveTypeKindGetter object ) throws IOException {}

    @Override
    public EdmPrimitiveTypeKindGetter read( ObjectDataInput in ) throws IOException {
        return EdmPrimitiveTypeKindGetter.GETTER;
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.EDM_PRIMITIVE_TYPE_KIND_GETTER.ordinal();
    }

    @Override
    public void destroy() {}

    @Override
    public Class<? extends EdmPrimitiveTypeKindGetter> getClazz() {
        return EdmPrimitiveTypeKindGetter.class;
    }

}
