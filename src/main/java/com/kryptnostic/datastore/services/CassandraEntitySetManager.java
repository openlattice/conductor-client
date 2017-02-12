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

package com.kryptnostic.datastore.services;

import java.util.UUID;

import com.auth0.jwt.internal.org.apache.commons.lang3.StringUtils;
import com.clearspring.analytics.util.Preconditions;
import com.dataloom.authorization.AuthorizationManager;
import com.dataloom.edm.EntitySet;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.google.common.collect.Iterables;
import com.kryptnostic.conductor.rpc.odata.Tables;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.datastore.cassandra.RowAdapters;

public class CassandraEntitySetManager {
    private final String               keyspace;
    private final Session              session;
    private final AuthorizationManager authorizations;

    private final PreparedStatement    getEntities;
    private final PreparedStatement    assignEntity;
    private final PreparedStatement    evictEntity;
    private final PreparedStatement    getEntitySetsByType;
    private final PreparedStatement    getEntitySet;
    private final Select               getAllEntitySets;

    public CassandraEntitySetManager( String keyspace, Session session, AuthorizationManager authorizations ) {
        Preconditions.checkArgument( StringUtils.isNotBlank( keyspace ), "Keyspace cannot be blank." );
        this.session = session;
        this.keyspace = keyspace;
        this.authorizations = authorizations;
        this.getEntitySet = session
                .prepare( QueryBuilder.select().all()
                        .from( this.keyspace, Tables.ENTITY_SETS.getName() )
                        .where( QueryBuilder.eq( CommonColumns.NAME.cql(), CommonColumns.NAME.bindMarker() ) ) );

        this.getEntitySetsByType = session
                .prepare( QueryBuilder.select().all()
                        .from( this.keyspace, Tables.ENTITY_SETS.getName() )
                        .where( QueryBuilder.eq( CommonColumns.ENTITY_TYPE_ID.cql(), CommonColumns.ENTITY_TYPE_ID.bindMarker() ) ) );

        this.getAllEntitySets = QueryBuilder.select().all().from( keyspace, Tables.ENTITY_SETS.getName() );
        this.getEntities = session
                .prepare( QueryBuilder.select().all()
                        .from( keyspace, Tables.ENTITY_ID_LOOKUP.getName() )
                        .where( QueryBuilder.eq( CommonColumns.ENTITY_SET_ID.cql(),
                                CommonColumns.ENTITY_SET_ID.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.SYNCID.cql(), CommonColumns.SYNCID.bindMarker() ) ) );
        this.assignEntity = session.prepare(
                QueryBuilder
                        .insertInto( keyspace, Tables.ENTITY_ID_LOOKUP.getName() )
                        .value( CommonColumns.ENTITY_SET_ID.cql(),
                                CommonColumns.ENTITY_SET_ID.bindMarker() )
                        .value( CommonColumns.SYNCID.cql(), CommonColumns.SYNCID.bindMarker() )
                        .value( CommonColumns.ENTITYID.cql(), CommonColumns.ENTITYID.bindMarker() ) );
        this.evictEntity = session.prepare(
                QueryBuilder
                        .delete().all()
                        .from( keyspace, Tables.ENTITY_ID_LOOKUP.getName() )
                        .where( QueryBuilder.eq( CommonColumns.ENTITY_SET_ID.cql(),
                                CommonColumns.ENTITY_SET_ID.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.SYNCID.cql(), CommonColumns.SYNCID.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.ENTITYID.cql(), CommonColumns.ENTITYID.bindMarker() ) ) );
    }

    public EntitySet getEntitySet( String entitySetName ) {
        Row row = session.execute( getEntitySet.bind().setString( CommonColumns.NAME.cql(), entitySetName ) ).one();
        return row == null ? null : RowAdapters.entitySet( row );
    }

    public Iterable<String> getEntitiesInEntitySet( UUID syncId, String entitySetName ) {
        ResultSet rs = session
                .execute( getEntities.bind()
                        .setUUID( CommonColumns.SYNCID.cql(), syncId )
                        .setUUID( CommonColumns.ENTITY_SET_ID.cql(),
                                getEntitySet( entitySetName ).getId() ) );
        return Iterables.transform( rs, row -> row.getString( CommonColumns.ENTITYID.cql() ) );
    }

    public void assignEntityToEntitySet( UUID syncId, String entityId, String entitySetName ) {
        session.execute(
                assignEntity.bind()
                        .setUUID( CommonColumns.SYNCID.cql(), syncId )
                        .setString( CommonColumns.ENTITYID.cql(), entityId )
                        .setString( CommonColumns.ENTITY_SET_ID.cql(), entitySetName ) );
    }

    public void evictEntityFromEntitySet( UUID syncId, String entityId, String entitySetName ) {
        session.execute(
                evictEntity.bind()
                        .setUUID( CommonColumns.SYNCID.cql(), syncId )
                        .setString( CommonColumns.ENTITYID.cql(), entityId )
                        .setString( CommonColumns.ENTITY_SET_ID.cql(), entitySetName ) );
    }

    public Iterable<EntitySet> getAllEntitySetsForType( UUID typeId ) {
        ResultSetFuture rsf = session.executeAsync(
                getEntitySetsByType.bind().setUUID( CommonColumns.ENTITY_TYPE_ID.cql(), typeId ) );
        return Iterables.transform( rsf.getUninterruptibly(), RowAdapters::entitySet );
    }

    public Iterable<EntitySet> getAllEntitySets() {
        ResultSetFuture rsf = session.executeAsync( getAllEntitySets );
        return Iterables.transform( rsf.getUninterruptibly(), RowAdapters::entitySet );
    }
}
