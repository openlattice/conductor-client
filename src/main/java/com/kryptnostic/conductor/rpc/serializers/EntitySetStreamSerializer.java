package com.kryptnostic.conductor.rpc.serializers;

import java.io.IOException;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.conductor.rpc.odata.EntitySet;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import com.kryptnostic.mapstores.v1.constants.HazelcastSerializerTypeIds;

public class EntitySetStreamSerializer implements SelfRegisteringStreamSerializer<EntitySet> {

	@Override
	public void write(ObjectDataOutput out, EntitySet object)
			throws IOException {
		out.writeUTF( object.getName() );
		new FullQualifiedNameStreamSerializer().write( out, object.getType() );
		out.writeUTF( object.getTitle() );
	}

	@Override
	public EntitySet read(ObjectDataInput in) throws IOException {
		EntitySet es = new EntitySet()
			.setName( in.readUTF() )
			.setType( new FullQualifiedNameStreamSerializer().read( in ) )
			.setTitle( in.readUTF() );
		return es;
	}

	@Override
	public int getTypeId() {
		return HazelcastSerializerTypeIds.ENTITY_SET.ordinal();
	}

	@Override
	public void destroy() {
		
	}

	@Override
	public Class<EntitySet> getClazz() {
		return EntitySet.class;
	}

}
