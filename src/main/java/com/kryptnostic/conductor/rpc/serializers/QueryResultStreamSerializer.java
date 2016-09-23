package com.kryptnostic.conductor.rpc.serializers;

import java.io.IOException;
import java.util.UUID;

import javax.inject.Inject;

import com.google.common.base.Optional;
import com.datastax.driver.core.Session;
import com.google.common.base.Preconditions;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.conductor.rpc.QueryResult;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import com.kryptnostic.services.v1.serialization.UUIDStreamSerializer;
import com.kryptnostic.mapstores.v1.constants.HazelcastSerializerTypeIds;

public class QueryResultStreamSerializer implements SelfRegisteringStreamSerializer<QueryResult> {
	private Session session;
	
	@Inject
	public QueryResultStreamSerializer( Session session ) {
		this.session = session;
	}
        
	@Override
	public void write(ObjectDataOutput out, QueryResult object) throws IOException {
		out.writeUTF( object.getKeyspace() );
		out.writeUTF( object.getTableName() );
		UUIDStreamSerializer.serialize( out, object.getQueryId() );
		out.writeUTF( object.getSessionId() );
	}

	@Override
	public QueryResult read(ObjectDataInput in) throws IOException {
		String keyspace = in.readUTF();
		String tableName = in.readUTF();
		UUID queryId = UUIDStreamSerializer.deserialize( in );
		String sessionId = in.readUTF();
		return new QueryResult( keyspace, tableName, queryId, sessionId, Optional.fromNullable( session ) );
	}

	@Override
	public int getTypeId() {
		return HazelcastSerializerTypeIds.QUERY_RESULT.ordinal();
	}

	@Override
	public void destroy() {
		
	}

	public synchronized void setSession( Session session ) {
		if( this.session == null ) {
			this.session = Preconditions.checkNotNull( session );
		}
	}

	@Override
	public Class<QueryResult> getClazz() {
		return QueryResult.class;
	}

}

