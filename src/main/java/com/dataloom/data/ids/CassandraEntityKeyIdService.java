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

package com.dataloom.data.ids;

import com.dataloom.data.EntityKey;
import com.dataloom.data.EntityKeyIdService;
import com.datastax.driver.core.*;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.kryptnostic.conductor.rpc.odata.Table;
import com.kryptnostic.datastore.cassandra.CommonColumns;

import java.util.Optional;
import java.util.UUID;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public class CassandraEntityKeyIdService implements EntityKeyIdService {

    private final ListeningExecutorService executor;

    private final Session           session;
    private final PreparedStatement insertNewId;
    private final PreparedStatement readEntityKey;

    public CassandraEntityKeyIdService(
            ListeningExecutorService executor,
            Session session,
            PreparedStatement insertNewId ) {
        this.executor = executor;
        this.session = session;
        this.insertNewId = insertNewId;
        this.insertNewId = prepareInsertIfNotExists( session );
        this.readEntityKey = prepareReadEntityKey( session );
    }

    @Override
    public Optional<EntityKey> getEntityKey( UUID entityKey ) {
        return Optional.ofNullable( getEntityKey( entityKey ) );
    }

    @Override
    public EntityKey getEntityKey( UUID entityKeyId ) {
        final Row row = getEntityKeyAsync( entityKeyId ).getUninterruptibly().one();
        if ( row != null ) {
            return row.get( CommonColumns.ENTITY_KEY.cql(), EntityKey.class );
        }
        return null;
    }

    @Override
    public ResultSetFuture getEntityKeyAsync( UUID entityKeyId ) {
        BoundStatement bs = readEntityKey.bind()
                .setUUID( CommonColumns.ID.cql(), entityKeyId );
        return session.executeAsync( bs );
    }

    @Override
    public ResultSetFuture setEntityKeyId(
            EntityKey entityKey, UUID entityKeyId ) {
        BoundStatement bs = insertNewId.bind()
                .setUUID( CommonColumns.ID.cql(), entityKeyId )
                .set( CommonColumns.ENTITY_KEY.cql(), entityKey, EntityKey.class );
        return session.executeAsync( bs );
    }

    private static PreparedStatement prepareReadEntityKey( Session session ) {
        return session.prepare( Table.IDS.getBuilder().buildLoadQuery() );
    }

    private static PreparedStatement prepareInsertIfNotExists( Session session ) {
        return session.prepare( Table.IDS.getBuilder().buildStoreQuery().ifNotExists() );
    }
}
