package com.kryptnostic.datastore.services;

import java.util.UUID;

import com.auth0.jwt.internal.org.apache.commons.lang3.StringUtils;
import com.clearspring.analytics.util.Preconditions;
import com.dataloom.authorization.AuthorizationManager;
import com.dataloom.edm.internal.EntitySet;
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
import com.kryptnostic.rhizome.cassandra.CassandraTableBuilder;

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
                        .where( QueryBuilder.eq( CommonColumns.TYPE.cql(), CommonColumns.TYPE.bindMarker() ) ) );

        this.getAllEntitySets = QueryBuilder.select().all().from( keyspace, Tables.ENTITY_SETS.getName() );
        this.getEntities = session
                .prepare( QueryBuilder.select().all()
                        .from( keyspace, Tables.ENTITIES.getName() )
                        .where(
                                QueryBuilder.contains( CommonColumns.ENTITY_SETS.cql(),
                                        CommonColumns.ENTITY_SETS.bindMarker() ) ) );
        this.assignEntity = session.prepare(
                QueryBuilder
                        .update( keyspace, Tables.ENTITIES.getName() )
                        .where( QueryBuilder.eq( CommonColumns.ENTITYID.cql(), CommonColumns.ENTITYID.bindMarker() ) )
                        .with( QueryBuilder.add( CommonColumns.ENTITY_SETS.cql(),
                                CommonColumns.ENTITY_SETS.bindMarker() ) ) );
        this.evictEntity = session.prepare(
                QueryBuilder
                        .update( keyspace, Tables.ENTITIES.getName() )
                        .where( QueryBuilder.eq( CommonColumns.ENTITYID.cql(), CommonColumns.ENTITYID.bindMarker() ) )
                        .with( QueryBuilder.remove( CommonColumns.ENTITY_SETS.cql(),
                                CommonColumns.ENTITY_SETS.bindMarker() ) ) );
    }

    public EntitySet getEntitySet( String entitySetName ) {
        Row row = session.execute( getEntitySet.bind().setString( CommonColumns.NAME.cql(), entitySetName ) ).one();
        return row == null ? null : RowAdapters.entitySet( row );
    }

    public Iterable<String> getEntitiesInEntitySet( String entitySetName ) {
        ResultSet rs = session
                .execute( getEntities.bind().setString( CommonColumns.ENTITY_SETS.cql(), entitySetName ) );
        return Iterables.transform( rs, row -> row.getString( CommonColumns.ENTITYID.cql() ) );
    }

    public void assignEntityToEntitySet( String entityId, String entitySetName ) {
        session.execute(
                assignEntity.bind()
                        .setString( CommonColumns.ENTITYID.cql(), entityId )
                        .setString( CommonColumns.ENTITY_SETS.cql(), entitySetName ) );
    }

    public void evictEntityFromEntitySet( String entityId, String entitySetName ) {
        session.execute(
                evictEntity.bind()
                        .setString( CommonColumns.ENTITYID.cql(), entityId )
                        .setString( CommonColumns.ENTITY_SETS.cql(), entitySetName ) );
    }

    public Iterable<EntitySet> getAllEntitySetsForType( UUID typeId ) {
        ResultSetFuture rsf = session.executeAsync(
                getEntitySetsByType.bind().setUUID( CommonColumns.TYPE_ID.cql(), typeId ) );
        return Iterables.transform( rsf.getUninterruptibly(), RowAdapters::entitySet );
    }

    public Iterable<EntitySet> getAllEntitySets() {
        ResultSetFuture rsf = session.executeAsync( getAllEntitySets );
        return Iterables.transform( rsf.getUninterruptibly(), RowAdapters::entitySet );
    }

    private static CassandraTableBuilder entitiesTable( String keyspace ) {
        return new CassandraTableBuilder( keyspace, Tables.ENTITIES.getName() )
                .partitionKey( CommonColumns.ENTITYID )
                .columns( CommonColumns.ENTITY_SETS );
    }
}
