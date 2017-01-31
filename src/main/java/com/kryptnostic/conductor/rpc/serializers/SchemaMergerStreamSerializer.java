package com.kryptnostic.conductor.rpc.serializers;

import java.io.IOException;
import java.util.Set;

import org.springframework.stereotype.Component;

import com.dataloom.edm.schemas.processors.SchemaMerger;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.hazelcast.serializers.SetStreamSerializers;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;

@Component
public class SchemaMergerStreamSerializer implements SelfRegisteringStreamSerializer<SchemaMerger> {

    @Override
    public void write( ObjectDataOutput out, SchemaMerger object ) throws IOException {
        SetStreamSerializers.serialize( out, object.getBackingCollection(), ( String name ) -> {
            out.writeUTF( name );
        } );
    }

    @Override
    public SchemaMerger read( ObjectDataInput in ) throws IOException {
        Set<String> names = SetStreamSerializers.deserialize( in, ( ObjectDataInput dataInput ) -> {
            return dataInput.readUTF();
        } );
        return new SchemaMerger( names );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.SCHEMA_MERGER.ordinal();
    }

    @Override
    public void destroy() {}

    @Override
    public Class<SchemaMerger> getClazz() {
        return SchemaMerger.class;
    }

}
