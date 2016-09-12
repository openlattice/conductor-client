package com.kryptnostic.datastore.services;

import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.olingo.commons.api.data.Entity;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.mapping.MappingManager;
import com.google.common.collect.ImmutableSet;
import com.hazelcast.core.HazelcastInstance;
import com.kryptnostic.conductor.rpc.UUIDs.ACLs;
import com.kryptnostic.conductor.rpc.odata.DatastoreConstants;
import com.kryptnostic.conductor.rpc.odata.EntitySet;
import com.kryptnostic.conductor.rpc.odata.EntityType;
import com.kryptnostic.conductor.rpc.odata.PropertyType;
import com.kryptnostic.conductor.rpc.odata.Schema;
import com.kryptnostic.conductor.rpc.odata.Tables;
import com.kryptnostic.datastore.cassandra.CassandraEdmMapping;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.datastore.cassandra.Queries;
import com.kryptnostic.datastore.util.Util;

import jersey.repackaged.com.google.common.base.Preconditions;
import jersey.repackaged.com.google.common.collect.Maps;

public class CassandraTableManager {
    static enum TableType {
        entity_,
        es_,
        index_,
        property_,
        schema_;
    }

    private final String                                              keyspace;
    private final Session                                             session;

    private final ConcurrentMap<FullQualifiedName, PreparedStatement> propertyTypeUpdateStatements;
    private final ConcurrentMap<FullQualifiedName, PreparedStatement> propertyIndexUpdateStatements;
    private final ConcurrentMap<FullQualifiedName, PreparedStatement> entityTypeInsertStatements;
    private final ConcurrentMap<FullQualifiedName, PreparedStatement> entityTypeUpdateStatements;
    private final ConcurrentMap<FullQualifiedName, PreparedStatement> entityIdToTypeUpdateStatements;

    private final ConcurrentMap<UUID, PreparedStatement>              schemaInsertStatements;
    private final ConcurrentMap<UUID, PreparedStatement>              schemaUpsertStatements;
    private final ConcurrentMap<UUID, PreparedStatement>              schemaSelectStatements;
    private final ConcurrentMap<UUID, PreparedStatement>              schemaSelectAllStatements;
    private final ConcurrentMap<UUID, PreparedStatement>              schemaSelectAllInNamespaceStatements;
    private final ConcurrentMap<UUID, PreparedStatement>              schemaAddEntityTypes;
    private final ConcurrentMap<UUID, PreparedStatement>              schemaRemoveEntityTypes;

    private final PreparedStatement                                   getTypenameForEntityType;
    private final PreparedStatement                                   getTypenameForPropertyType;
    private final PreparedStatement                                   countProperty;
    private final PreparedStatement                                   countEntitySets;
    private final PreparedStatement                                   insertPropertyTypeLookup;
    private final PreparedStatement                                   updatePropertyTypeLookup;
    private final PreparedStatement                                   deletePropertyTypeLookup;
    private final PreparedStatement                                   getFullQualifiedNameForTypename;
    private final PreparedStatement                                   getTypenameForEntityId;
    private final PreparedStatement                                   assignEntityToEntitySet;

    public CassandraTableManager(
            HazelcastInstance hazelcast,
            String keyspace,
            Session session,
            MappingManager mm ) {
        this.session = session;
        this.keyspace = keyspace;

        this.propertyTypeUpdateStatements = Maps.newConcurrentMap();
        this.propertyIndexUpdateStatements = Maps.newConcurrentMap();
        this.entityTypeInsertStatements = Maps.newConcurrentMap();
        this.entityTypeUpdateStatements = Maps.newConcurrentMap();
        this.schemaInsertStatements = Maps.newConcurrentMap();
        this.schemaUpsertStatements = Maps.newConcurrentMap();
        this.schemaSelectStatements = Maps.newConcurrentMap();
        this.schemaSelectAllStatements = Maps.newConcurrentMap();
        this.schemaSelectAllInNamespaceStatements = Maps.newConcurrentMap();
        this.schemaAddEntityTypes = Maps.newConcurrentMap();
        this.schemaRemoveEntityTypes = Maps.newConcurrentMap();
        this.entityIdToTypeUpdateStatements = Maps.newConcurrentMap();

        initCoreTables( keyspace, session );
        prepareSchemaQueries();

        this.getTypenameForEntityType = session.prepare( QueryBuilder
                .select()
                .from( keyspace, Tables.ENTITY_TYPES.getTableName() )
                .where( QueryBuilder.eq( CommonColumns.NAMESPACE.cql(),
                        QueryBuilder.bindMarker() ) )
                .and( QueryBuilder.eq( CommonColumns.NAME.cql(),
                        QueryBuilder.bindMarker() ) ) );

        this.getTypenameForPropertyType = session.prepare( QueryBuilder
                .select()
                .from( keyspace, Tables.PROPERTY_TYPES.getTableName() )
                .where( QueryBuilder.eq( CommonColumns.NAMESPACE.cql(),
                        QueryBuilder.bindMarker() ) )
                .and( QueryBuilder.eq( CommonColumns.NAME.cql(),
                        QueryBuilder.bindMarker() ) ) );

        this.countProperty = session.prepare( QueryBuilder
                .select().countAll()
                .from( keyspace, DatastoreConstants.PROPERTY_TYPES_TABLE )
                .where( QueryBuilder.eq( CommonColumns.NAMESPACE.cql(),
                        QueryBuilder.bindMarker() ) )
                .and( QueryBuilder.eq( CommonColumns.NAME.cql(),
                        QueryBuilder.bindMarker() ) ) );

        this.countEntitySets = session.prepare( Queries.countEntitySets( keyspace ) );

        this.insertPropertyTypeLookup = session
                .prepare( QueryBuilder.insertInto( keyspace, Tables.FQN_LOOKUP.getTableName() )
                        .value( CommonColumns.TYPENAME.cql(), QueryBuilder.bindMarker() )
                        .value( CommonColumns.FQN.cql(), QueryBuilder.bindMarker() ) );

        this.updatePropertyTypeLookup = session
                .prepare( ( QueryBuilder.update( keyspace, Tables.FQN_LOOKUP.getTableName() ) )
                        .with( QueryBuilder.set( CommonColumns.FQN.cql(), QueryBuilder.bindMarker() ) )
                        .where( QueryBuilder.eq( CommonColumns.TYPENAME.cql(), QueryBuilder.bindMarker() ) ) );

        this.deletePropertyTypeLookup = session
                .prepare( QueryBuilder.delete().from( keyspace, Tables.FQN_LOOKUP.getTableName() )
                        .where( QueryBuilder.eq( CommonColumns.TYPENAME.cql(), QueryBuilder.bindMarker() ) ) );

        //property type
        this.getFullQualifiedNameForTypename = session
                .prepare( QueryBuilder.select().from( keyspace, Tables.FQN_LOOKUP.getTableName() )
                        .where( QueryBuilder.eq( CommonColumns.TYPENAME.cql(), QueryBuilder.bindMarker() ) ) );
        
        this.getTypenameForEntityId = session
                .prepare( QueryBuilder.select().from( keyspace, Tables.ENTITY_ID_TO_TYPE.getTableName() )
                        .where( QueryBuilder.eq( CommonColumns.ENTITYID.cql(), QueryBuilder.bindMarker() ) ) );
        
        this.assignEntityToEntitySet = session
                .prepare( QueryBuilder.insertInto(keyspace, Tables.ENTITY_SET_MEMBERS.getTableName() )
                        .value( CommonColumns.TYPENAME.cql(), QueryBuilder.bindMarker() )
                        .value( CommonColumns.NAME.cql(), QueryBuilder.bindMarker() )
                        .value( CommonColumns.PARTITION_INDEX.cql(), QueryBuilder.bindMarker() )
                        .value( CommonColumns.ENTITYID.cql(), QueryBuilder.bindMarker() ) );
    }

    public String getKeyspace() {
        return keyspace;
    }

    public boolean createSchemaTableForAclId( UUID aclId ) {
        if ( createSchemasTableIfNotExists( keyspace, aclId, session ) ) {
            prepareSchemaQuery( aclId );
            return true;
        }
        return false;
    }

    public PreparedStatement getSchemaInsertStatement( UUID aclId ) {
        return schemaInsertStatements.get( aclId );
    }

    /**
     * Get prepared statement for loading schemas from the various tables based on aclId.
     *
     * @param aclId
     * @return
     */
    public PreparedStatement getSchemaStatement( UUID aclId ) {
        return schemaSelectStatements.get( aclId );
    }

    public PreparedStatement getSchemasInNamespaceStatement( UUID aclId ) {
        return schemaSelectAllInNamespaceStatements.get( aclId );
    }

    public PreparedStatement getAllSchemasStatement( UUID aclId ) {
        return schemaSelectAllStatements.get( aclId );
    }

    public PreparedStatement getSchemaUpsertStatement( UUID aclId ) {
        return schemaUpsertStatements.get( aclId );
    }

    public PreparedStatement getSchemaAddEntityTypeStatement( UUID aclId ) {
        return schemaAddEntityTypes.get( aclId );
    }

    public PreparedStatement getSchemaRemoveEntityTypeStatement( UUID aclId ) {
        return schemaRemoveEntityTypes.get( aclId );
    }

    public void registerSchema( Schema schema ) {
        Preconditions.checkArgument( schema.getEntityTypeFqns().size() == schema.getEntityTypes().size(),
                "Schema is out of sync." );
        schema.getEntityTypes().forEach( et -> {
            // TODO: Solve ID generation
            /*
             * While unlikely it's possible to have a UUID collision when creating an object. Two possible solutions:
             * (1) Use Hazelcast and perform a read prior to every write (2) Maintain a self-refreshing in-memory pool
             * of available UUIDs that shifts the reads to times when cassandra is under less stress. Option (2) with a
             * fall back to random UUID generation when pool is exhausted seems like an efficient bet.
             */
            putEntityTypeInsertStatement( et.getFullQualifiedName() );
            putEntityTypeUpdateStatement( et.getFullQualifiedName() );
            putEntityIdToTypeUpdateStatement( et.getFullQualifiedName() );
            et.getKey().forEach( fqn -> putPropertyIndexUpdateStatement( fqn ) );
        } );

        schema.getPropertyTypes().forEach( pt -> {
            putPropertyTypeUpdateStatement( pt.getFullQualifiedName() );
        } );
    }

    public PreparedStatement getInsertEntityPreparedStatement( EntityType entityType ) {
        return getInsertEntityPreparedStatement( entityType.getFullQualifiedName() );
    }

    public PreparedStatement getInsertEntityPreparedStatement( FullQualifiedName fqn ) {
        return entityTypeInsertStatements.get( fqn );
    }

    public PreparedStatement getUpdateEntityPreparedStatement( EntityType entityType ) {
        return getUpdateEntityPreparedStatement( entityType.getFullQualifiedName() );
    }

    public PreparedStatement getUpdateEntityPreparedStatement( FullQualifiedName fqn ) {
        return entityTypeUpdateStatements.get( fqn );
    }

    public PreparedStatement getUpdateEntityIdTypenamePreparedStatement( FullQualifiedName fqn ) {
        return entityIdToTypeUpdateStatements.get( fqn );
    }

    public PreparedStatement getUpdatePropertyPreparedStatement( PropertyType propertyType ) {
        return getUpdatePropertyPreparedStatement( propertyType.getFullQualifiedName() );
    }

    public PreparedStatement getUpdatePropertyPreparedStatement( FullQualifiedName fqn ) {
        return propertyTypeUpdateStatements.get( fqn );
    }

    public PreparedStatement getUpdatePropertyIndexPreparedStatement( FullQualifiedName fqn ) {
        return this.propertyIndexUpdateStatements.get( fqn );
    }

    public PreparedStatement getCountPropertyStatement() {
        return countProperty;
    }

    public PreparedStatement getCountEntitySetsStatement() {
        return countEntitySets;
    }

    public void createEntityTypeTable( EntityType entityType, Map<FullQualifiedName, PropertyType> keyPropertyTypes ) {
        // Ensure that type name doesn't exist
        String entityTableQuery;

        do {
            entityTableQuery = Queries.createEntityTable( keyspace, getTablenameForEntityType( entityType ) );
        } while ( !Util.wasLightweightTransactionApplied( session.execute( entityTableQuery ) ) );

        entityType.getKey().forEach( fqn -> {
            // TODO: Use elasticsearch for maintaining index instead of maintaining in Cassandra.
            /*
             * This makes sure that index tables are created if they do not exist. Other entity types may already be
             * using this property type as a key.
             */
            PropertyType keyPropertyType = keyPropertyTypes.get( fqn );
            String typename = keyPropertyType.getTypename();
            Preconditions.checkArgument( StringUtils.isNotBlank( typename ),
                    "Typename for key property type cannot be null" );
            session.execute( Queries.createPropertyTableQuery( keyspace,
                    getTablenameForPropertyIndex( keyPropertyType ),
                    cc -> CassandraEdmMapping.getCassandraType( keyPropertyType.getDatatype() ) ) );

            putPropertyIndexUpdateStatement( fqn );
        } );
        putEntityTypeInsertStatement( entityType.getFullQualifiedName() );
        putEntityTypeUpdateStatement( entityType.getFullQualifiedName() );
        putEntityIdToTypeUpdateStatement( entityType.getFullQualifiedName() );
        // Loop until table creation succeeds.
    }

    public void createPropertyTypeTable( PropertyType propertyType ) {
        String propertyTableQuery;
        DataType valueType = CassandraEdmMapping.getCassandraType( propertyType.getDatatype() );
        do {
            propertyTableQuery = Queries.createPropertyTableQuery( keyspace,
                    getTablenameForPropertyValuesOfType( propertyType ),
                    cc -> valueType );
            // Loop until table creation succeeds.
        } while ( !Util.wasLightweightTransactionApplied( session.execute( propertyTableQuery ) ) );

        putPropertyTypeUpdateStatement( propertyType.getFullQualifiedName() );
    }

    public void deleteEntityTypeTable( String namespace, String entityName ) {
        // We should mark tables for deletion-- we lose historical information if we hard delete properties.
        /*
         * Use Accessor interface to look up objects and retrieve typename corresponding to table to delete.
         */
        throw new NotImplementedException( "Blame MTR" );//TODO
    }

    public void deletePropertyTypeTable( String namespace, String propertyName ) {
        throw new NotImplementedException( "Blame MTR" );//TODO
    }

    /**
     * Operations on Typename to (user-friendly) FullQualifiedName Lookup Table
     */

    public void insertToFQNLookupTable( PropertyType propertyType ) {
        session.execute(
                insertPropertyTypeLookup.bind( propertyType.getTypename(), propertyType.getFullQualifiedName() ) );
    }

    public void updateFQNLookupTable( PropertyType propertyType ) {
        session.execute(
                updatePropertyTypeLookup.bind( propertyType.getTypename(), propertyType.getFullQualifiedName() ) );
        //TODO: reorder binding?
    }

    public void deleteFromFQNTable( PropertyType propertyType ) {
        FullQualifiedName fqn = getFullQualifiedNameForTypename( propertyType.getTypename() );
        if ( fqn != null ) {
            session.execute(
                    deletePropertyTypeLookup.bind( propertyType.getTypename() ) );
        }
    }

    /**
     * Name getters for Entity Type
     */

    public String getTypenameForEntityType( EntityType entityType ) {
        return getTypenameForEntityType( entityType.getNamespace(), entityType.getName() );
    }

    public String getTypenameForEntityType( FullQualifiedName fullQualifiedName ) {
        return getTypenameForEntityType( fullQualifiedName.getNamespace(), fullQualifiedName.getName() );
    }

    public String getTypenameForEntityType( String namespace, String name ) {
        return Util.transformSafely( session.execute( this.getTypenameForEntityType.bind( namespace, name ) ).one(),
                r -> r.getString( CommonColumns.TYPENAME.cql() ) );
    }
    
    public String getTypenameForEntitySet( EntitySet entitySet ) {
        return getTypenameForEntityType( entitySet.getType() );
    }

    public String getTablenameForEntityType( EntityType entityType ) {
        // `If type name is provided then just directly return the table name
        final String typename = entityType.getTypename();
        if ( StringUtils.isNotBlank( typename ) ) {
            return getTablenameForEntityTypeFromTypenameAndAclId( ACLs.EVERYONE_ACL, typename );
        }
        return getTablenameForEntityType( entityType.getFullQualifiedName() );
    }

    public String getTablenameForEntityType( FullQualifiedName fqn ) {
        return getTablenameForEntityTypeFromTypenameAndAclId( ACLs.EVERYONE_ACL, getTypenameForEntityType( fqn ) );
    }

    public String getTablenameForEntityTypeFromTypenameAndAclId( UUID aclId, String typename ) {
        return getTablename( TableType.entity_, aclId, typename );
    }

    public Boolean assignEntityToEntitySet( UUID entityId, String typename, String name ) {
        SecureRandom random = new SecureRandom();
        return Util.wasLightweightTransactionApplied( 
                session.execute( 
                        assignEntityToEntitySet.bind( 
                                typename,
                                name, 
                                Arrays.toString(random.generateSeed(1)), 
                                entityId )));
    }
    
    public Boolean assignEntityToEntitySet( UUID entityId, EntitySet es ) {
        String typename = getTypenameForEntitySet( es );
        SecureRandom random = new SecureRandom();
        return Util.wasLightweightTransactionApplied( 
                session.execute( 
                        assignEntityToEntitySet.bind( 
                                typename,
                                es.getName(), 
                                Arrays.toString(random.generateSeed(1)), 
                                entityId )));
    }
    
    public String getTypenameForEntityId( UUID entityId ) {        
        return Util.transformSafely( session.execute( this.getTypenameForEntityId.bind( entityId ) ).one(),
                r -> r.getString( CommonColumns.TYPENAME.cql() ) );
    }

    /*************************
     Getters for Property Type
     *************************/

    public String getTypenameForPropertyType( PropertyType propertyType ) {
        return getTypenameForPropertyType( propertyType.getNamespace(), propertyType.getName() );
    }

    public String getTypenameForPropertyType( FullQualifiedName fullQualifiedName ) {
        return getTypenameForPropertyType( fullQualifiedName.getNamespace(), fullQualifiedName.getName() );
    }

    private String getTypenameForPropertyType( String namespace, String name ) {
        return Util.transformSafely( session.execute( this.getTypenameForPropertyType.bind( namespace, name ) ).one(),
                r -> r.getString( CommonColumns.TYPENAME.cql() ) );
    }

    public String getTablenameForPropertyValuesOfType( PropertyType propertyType ) {
        // If type name is provided then just directly return the table name
        final String typename = propertyType.getTypename();
        if ( StringUtils.isNotBlank( typename ) ) {
            return getTablenameForPropertyValuesFromTypenameAndAclId( ACLs.EVERYONE_ACL, typename );
        }
        return getTablenameForPropertyValuesOfType( propertyType.getFullQualifiedName() );
    }

    public String getTablenameForPropertyValuesOfType( FullQualifiedName propertyFqn ) {
        return getTablenameForPropertyValuesFromTypenameAndAclId( ACLs.EVERYONE_ACL,
                getTypenameForPropertyType( propertyFqn ) );
    }

    public String getTablenameForPropertyValuesFromTypenameAndAclId( UUID aclId, String typename ) {
        return getTablename( TableType.property_, aclId, typename );
    }

    public String getTablenameForPropertyIndex( PropertyType propertyType ) {
        final String typename = propertyType.getTypename();
        if ( StringUtils.isNotBlank( typename ) ) {
            return getTablenameForPropertyIndexFromTypenameAndAclId( ACLs.EVERYONE_ACL, typename );
        }
        return getTablenameForPropertyIndexOfType( propertyType.getFullQualifiedName() );
    }

    public String getTablenameForPropertyIndexOfType( FullQualifiedName propertyFqn ) {
        return getTablenameForPropertyIndexFromTypenameAndAclId( ACLs.EVERYONE_ACL,
                getTypenameForPropertyType( propertyFqn ) );
    }

    public String getTablenameForPropertyIndexFromTypenameAndAclId( UUID aclId, String typename ) {
        return getTablename( TableType.index_, aclId, typename );
    }

    public Map<String, FullQualifiedName> getFullQualifiedNamesForTypenames( Iterable<String> typenames ) {
        return Maps.toMap( typenames, this::getFullQualifiedNameForTypename );
    }

    public FullQualifiedName getFullQualifiedNameForTypename( String typename ) {
        return Util.transformSafely( session.execute( this.getFullQualifiedNameForTypename.bind( typename ) ).one(),
                r -> new FullQualifiedName( r.getString( CommonColumns.FQN.cql() ) ) );
    }

    private void putEntityIdToTypeUpdateStatement( FullQualifiedName entityTypeFqn ) {
        entityIdToTypeUpdateStatements.put( entityTypeFqn,
                session.prepare( QueryBuilder
                        .update( keyspace, Tables.ENTITY_ID_TO_TYPE.getTableName() )
                        .with( QueryBuilder.set( CommonColumns.TYPENAME.cql(), QueryBuilder.bindMarker() ) )
                        .where( QueryBuilder.eq( CommonColumns.ENTITYID.cql(), QueryBuilder.bindMarker() ) ) ) );
    }

    private void putEntityTypeInsertStatement( FullQualifiedName entityTypeFqn ) {
        entityTypeInsertStatements.put( entityTypeFqn,
                session.prepare( QueryBuilder
                        .insertInto( keyspace, getTablenameForEntityType( entityTypeFqn ) )
                        .value( CommonColumns.ENTITYID.cql(), QueryBuilder.bindMarker() )
                        .value( CommonColumns.CLOCK.cql(),
                                QueryBuilder.fcall( "toTimestamp", QueryBuilder.now() ) )
                        .value( CommonColumns.TYPENAME.cql(), QueryBuilder.bindMarker() )
                        .value( CommonColumns.ENTITYSETS.cql(), QueryBuilder.bindMarker() )
                        .value( CommonColumns.SYNCIDS.cql(), QueryBuilder.bindMarker() ) ) );
    }

    private void putEntityTypeUpdateStatement( FullQualifiedName entityTypeFqn ) {
        entityTypeUpdateStatements.put( entityTypeFqn,
                session.prepare( QueryBuilder
                        .update( keyspace, getTablenameForEntityType( entityTypeFqn ) )
                        .with( QueryBuilder.set( CommonColumns.TYPENAME.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.addAll( CommonColumns.ENTITYSETS.cql(),
                                QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.appendAll( CommonColumns.SYNCIDS.cql(),
                                QueryBuilder.bindMarker() ) )
                        .where( QueryBuilder.eq( CommonColumns.ENTITYID.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.CLOCK.cql(), QueryBuilder.bindMarker() ) ) ) );
    }

    private void putPropertyTypeUpdateStatement( FullQualifiedName propertyTypeFqn ) {
        // The preparation process re-orders the bind markers. Below they are set according to the order that they get
        // mapped to
        propertyTypeUpdateStatements.put( propertyTypeFqn,
                session.prepare( QueryBuilder
                        .update( keyspace, getTablenameForPropertyValuesOfType( propertyTypeFqn ) )
                        .with( QueryBuilder.appendAll( CommonColumns.SYNCIDS.cql(),
                                QueryBuilder.bindMarker() ) )
                        .where( QueryBuilder.eq( CommonColumns.ENTITYID.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.VALUE.cql(), QueryBuilder.bindMarker() ) ) ) );

    }

    private void putPropertyIndexUpdateStatement( FullQualifiedName propertyTypeFqn ) {
        // The preparation process re-orders the bind markers. Below they are set according to the order that they get
        // mapped to
        propertyIndexUpdateStatements.put( propertyTypeFqn,
                session.prepare( QueryBuilder
                        .update( keyspace, getTablenameForPropertyIndexOfType( propertyTypeFqn ) )
                        .with( QueryBuilder.appendAll( CommonColumns.SYNCIDS.cql(),
                                QueryBuilder.bindMarker() ) )
                        .where( QueryBuilder.eq( CommonColumns.VALUE.cql(), QueryBuilder.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.ENTITYID.cql(), QueryBuilder.bindMarker() ) ) ) );
    }

    private void prepareSchemaQueries() {
        Set<UUID> aclIds = getAclsAppliedToSchemas();
        aclIds.forEach( this::prepareSchemaQuery );
    }

    private void prepareSchemaQuery( UUID aclId ) {
        String table = getTablenameForSchemaWithAclId( aclId );
        schemaSelectAllStatements.put( aclId, session.prepare( Queries.getAllSchemasQuery( keyspace, table ) ) );
        schemaSelectAllInNamespaceStatements.put( aclId,
                session.prepare( Queries.getAllSchemasInNamespaceQuery( keyspace, table ) ) );
        schemaSelectStatements.put( aclId, session.prepare( Queries.getSchemaQuery( keyspace, table ) ) );
        schemaInsertStatements.put( aclId, session.prepare( Queries.insertSchemaQueryIfNotExists( keyspace, table ) ) );
        schemaUpsertStatements.put( aclId, session.prepare( Queries.insertSchemaQuery( keyspace, table ) ) );
        schemaAddEntityTypes.putIfAbsent( aclId, session.prepare( Queries.addEntityTypesToSchema( keyspace, table ) ) );
        schemaRemoveEntityTypes.putIfAbsent( aclId,
                session.prepare( Queries.removeEntityTypesToSchema( keyspace, table ) ) );
    }

    private Set<UUID> getAclsAppliedToSchemas() {
        return ImmutableSet.of( ACLs.EVERYONE_ACL );
    }

    private void initCoreTables( String keyspace, Session session ) {
        createKeyspaceSparksIfNotExists( keyspace, session );
        createSchemaAclsTableIfNotExists( keyspace, session );
        createEntityTypesTableIfNotExists( keyspace, session );
        createPropertyTypesTableIfNotExists( keyspace, session );
        createEntitySetsTableIfNotExists( keyspace, session );
        createEntitySetMembersTableIfNotExists( keyspace, session );
        createFullQualifiedNameLookupTableIfNotExists( keyspace, session );
        createEntityIdTypenameTableIfNotExists( keyspace, session );
        // TODO: Remove this once everyone ACL is baked in.
        createSchemaTableForAclId( ACLs.EVERYONE_ACL );
    }

    public static String getTablenameForSchemaWithAclId( UUID aclId ) {
        return getTablename( TableType.schema_, aclId, "" );
    }

    public static String getTablename( TableType tableType, UUID aclId, String suffix ) {
        return tableType.name() + Util.toUnhyphenatedString( aclId ) + "_" + suffix;
    }

    public static String generateTypename() {
        return RandomStringUtils.randomAlphanumeric( 24 ).toLowerCase();
    }

    /**************
     Table Creators
     **************/

    private static void createKeyspaceSparksIfNotExists( String keyspace, Session session ) {
        session.execute( Queries.CREATE_KEYSPACE );
    }

    private static void createSchemaAclsTableIfNotExists( String keyspace, Session session ) {
        session.execute( Queries.createSchemaAclsTableQuery( keyspace ) );
    }

    private static boolean createSchemasTableIfNotExists( String keyspace, UUID aclId, Session session ) {
        return Util.wasLightweightTransactionApplied( session
                .execute( Queries.createSchemasTableQuery( keyspace, getTablenameForSchemaWithAclId( aclId ) ) ) );
    }

    private static void createEntitySetsTableIfNotExists( String keyspace, Session session ) {
        session.execute( Queries.getCreateEntitySetsTableQuery( keyspace ) );
        session.execute( Queries.CREATE_INDEX_ON_NAME );
    }

    private static void createEntitySetMembersTableIfNotExists( String keyspace, Session session ) {
        session.execute( Queries.getCreateEntitySetMembersTableQuery( keyspace ) );
    }
    
    private static void createEntityIdTypenameTableIfNotExists( String keyspace, Session session ) {
        session.execute( Queries.getCreateEntityIdToTypenameTableQuery( keyspace ) );
    }

    private static void createEntityTypesTableIfNotExists( String keyspace, Session session ) {
        session.execute( Queries.getCreateEntityTypesTableQuery( keyspace ) );
    }

    private void createPropertyTypesTableIfNotExists( String keyspace, Session session ) {
        session.execute( Queries.getCreatePropertyTypesTableQuery( keyspace ) );
    }

    private void createFullQualifiedNameLookupTableIfNotExists( String keyspace, Session session ) {
        session.execute( Queries.getCreateFqnLookupTableQuery( keyspace ) );
    }

}
