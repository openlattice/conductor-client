package com.kryptnostic.conductor.rpc;

import java.io.Serializable;
import java.util.UUID;

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
				" sessionid" );
	}

	@Override
	protected QueryResultStreamSerializer createSerializer() {
		return new QueryResultStreamSerializer( null );
	}

}
