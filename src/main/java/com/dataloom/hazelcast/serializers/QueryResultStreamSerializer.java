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
import java.util.UUID;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.datastax.driver.core.Session;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.conductor.rpc.QueryResult;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;

@Component
public class QueryResultStreamSerializer implements SelfRegisteringStreamSerializer<QueryResult> {
	private Session session;

    @Inject
    public QueryResultStreamSerializer( Session session ) {
        this.session = session;
    }

    @Override
    public void write( ObjectDataOutput out, QueryResult object ) throws IOException {
        out.writeUTF( object.getKeyspace() );
        out.writeUTF( object.getTableName() );
        UUIDStreamSerializer.serialize( out, object.getQueryId() );
        out.writeUTF( object.getSessionId() );
    }

    @Override
    public QueryResult read( ObjectDataInput in ) throws IOException {
        String keyspace = in.readUTF();
        String tableName = in.readUTF();
        UUID queryId = UUIDStreamSerializer.deserialize( in );
        String sessionId = in.readUTF();
        return new QueryResult( keyspace, tableName, queryId, sessionId, Optional.fromNullable( session ) );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.QUERY_RESULT.ordinal();
    }

    @Override
    public void destroy() {

    }

    public synchronized void setSession( Session session ) {
        if ( this.session == null ) {
            this.session = Preconditions.checkNotNull( session );
        }
    }

    @Override
    public Class<QueryResult> getClazz() {
        return QueryResult.class;
    }

}
