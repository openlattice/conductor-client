package com.kryptnostic.conductor.rpc.serializers;

import java.io.IOException;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;

public class FullQualifiedNameStreamSerializer implements SelfRegisteringStreamSerializer<FullQualifiedName> {

	@Override
	public void write(ObjectDataOutput out, FullQualifiedName object)
			throws IOException {
		serialize( out, object );
	}

	@Override
	public FullQualifiedName read(ObjectDataInput in) throws IOException {
		return deserialize( in );
	}

    @Override
	public int getTypeId() {
		return StreamSerializerTypeIds.FULL_QUALIFIED_NAME.ordinal();
	}

	@Override
	public void destroy() {
		
	}

	@Override
	public Class<FullQualifiedName> getClazz() {
		return FullQualifiedName.class;
	}
	
	public static void serialize( ObjectDataOutput out, FullQualifiedName object ) throws IOException {
	    out.writeUTF( object.getNamespace() );
        out.writeUTF( object.getName() );
	}
	
	public static  FullQualifiedName  deserialize( ObjectDataInput in ) throws IOException {
        String namespace = in.readUTF();
        String name = in.readUTF();
        return new FullQualifiedName( namespace, name );
        
    }
}
