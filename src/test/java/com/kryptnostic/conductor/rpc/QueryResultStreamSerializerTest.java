package com.kryptnostic.conductor.rpc;

import java.io.Serializable;
import java.util.UUID;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.kryptnostic.conductor.rpc.odata.EntitySet;
import com.kryptnostic.conductor.rpc.serializers.QueryResultStreamSerializer;
import com.kryptnostic.rhizome.hazelcast.serializers.BaseSerializerTest;

public class QueryResultStreamSerializerTest extends BaseSerializerTest<QueryResultStreamSerializer, QueryResult>
implements Serializable {
	private static final long serialVersionUID = -8582472573746218921L;

	@Override
	protected QueryResult createInput() {
		return new QueryResult(
				"namespace",
				"table",
				UUID.randomUUID(),
				" sessionid",
				new EntitySet()
					.setName( "esname")
					.setType(new FullQualifiedName( "foo", "bar" ) )
					.setTitle( "yay" ) );
	}

	@Override
	protected QueryResultStreamSerializer createSerializer() {
		return new QueryResultStreamSerializer( null );
	}

}
