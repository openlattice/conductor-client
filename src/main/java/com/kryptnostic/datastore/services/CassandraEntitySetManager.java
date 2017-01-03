package com.kryptnostic.datastore.services;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.auth0.jwt.internal.org.apache.commons.lang3.StringUtils;
import com.clearspring.analytics.util.Preconditions;
import com.dataloom.edm.internal.EntitySet;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.google.common.collect.Iterables;
import com.kryptnostic.conductor.rpc.odata.Tables;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.datastore.cassandra.Queries;
import com.kryptnostic.datastore.cassandra.RowAdapters;
import com.kryptnostic.rhizome.cassandra.CassandraTableBuilder;
import static com.google.common.base.Preconditions.checkNotNull;

public class CassandraEntitySetManager {
    private final Session           session;
    private final String            keyspace;

    private final PreparedStatement getEntities;
    private final PreparedStatement assignEntity;
    private final PreparedStatement getEntitySetsByType;
    private final Select            getAllEntitySets;

    public CassandraEntitySetManager( Session session, String keyspace ) {
        Preconditions.checkArgument( StringUtils.isNotBlank( keyspace ), "Keyspace cannot be blank." );
        createEntitySetsTableIfNotExists( keyspace, checkNotNull( session ) );
        this.session = session;
        this.keyspace = keyspace;
        this.getEntitySetsByType = session
                .prepare( QueryBuilder.select().from( this.keyspace, Tables.ENTITY_SETS.getName() )
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

    public Iterable<EntitySet> getAllEntitySetsForType( FullQualifiedName type ) {
        ResultSetFuture rsf = session.executeAsync(
                getEntitySetsByType.bind().set( CommonColumns.TYPE.cql(), type, FullQualifiedName.class ) );
        return Iterables.transform( rsf.getUninterruptibly(), RowAdapters::entitySet );
    }

    public Iterable<EntitySet> getAllEntitySets() {
        ResultSetFuture rsf = session.executeAsync( getAllEntitySets );
        return Iterables.transform( rsf.getUninterruptibly(), RowAdapters::entitySet );
    }

    private static void createEntitySetsTableIfNotExists( String keyspace, Session session ) {
        session.execute( Queries.getCreateEntitySetsTableQuery( keyspace ) );
        session.execute( Queries.CREATE_INDEX_ON_NAME );
        session.execute( entitiesTable( keyspace ).buildQuery() );
    }

    private static CassandraTableBuilder entitiesTable( String keyspace ) {
        return new CassandraTableBuilder( keyspace, Tables.ENTITIES.getName() )
                .partitionKey( CommonColumns.ENTITYID )
                .columns( CommonColumns.ENTITY_SETS );
    }
}
