package com.kryptnostic.conductor.rpc.odata;

import com.dataloom.edm.internal.DatastoreConstants;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.rhizome.cassandra.CassandraTableBuilder;
import com.kryptnostic.rhizome.cassandra.TableDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.EnumMap;

public enum Tables implements TableDef {
    ACL_KEYS,
    AUDIT_EVENTS,
    AUDIT_METRICS,
    DATA,
    ENTITY_ID_LOOKUP,
    ENTITY_SETS,
    ENTITY_TYPES,
    NAMES,
    ORGANIZATIONS,
    PERMISSIONS,
    PERMISSIONS_REQUESTS_UNRESOLVED,
    PERMISSIONS_REQUESTS_RESOLVED,
    PROPERTY_TYPES,
    SCHEMAS,
    REQUESTS;

    private static final Logger                                 logger   = LoggerFactory
            .getLogger( Tables.class );
    private static final EnumMap<Tables, CassandraTableBuilder> cache    = new EnumMap<>( Tables.class );
    private static       String                                 keyspace = DatastoreConstants.KEYSPACE;

    static CassandraTableBuilder getTableDefinition( Tables table ) {
        CassandraTableBuilder ctb = cache.get( table );
        if ( ctb == null ) {
            ctb = createTableDefinition( table );
            cache.put( table, ctb );
        }
        return ctb;
    }

    static CassandraTableBuilder createTableDefinition( Tables table ) {
        switch ( table ) {
            case ACL_KEYS:
                return new CassandraTableBuilder( ACL_KEYS )
                        .ifNotExists()
                        .partitionKey( CommonColumns.NAME )
                        .columns( CommonColumns.SECURABLE_OBJECTID );
            case AUDIT_EVENTS:
                return new CassandraTableBuilder( AUDIT_EVENTS )
                        .ifNotExists()
                        .partitionKey( CommonColumns.ACL_KEYS )
                        .clusteringColumns( CommonColumns.TIME_ID, CommonColumns.PRINCIPAL_TYPE, CommonColumns.PRINCIPAL_ID )
                        .columns( CommonColumns.PERMISSIONS, CommonColumns.BLOCK )
                        .staticColumns( CommonColumns.SECURABLE_OBJECT_TYPE )
                        .sasi( CommonColumns.PRINCIPAL_TYPE, CommonColumns.PRINCIPAL_ID );
            case AUDIT_METRICS:
                return new CassandraTableBuilder( AUDIT_METRICS )
                        .ifNotExists()
                        .partitionKey( CommonColumns.ACL_KEYS )
                        .clusteringColumns( CommonColumns.COUNT, CommonColumns.ACL_KEY_VALUE )
                        .withDescendingOrder( CommonColumns.COUNT );
            case ENTITY_ID_LOOKUP:
                return new CassandraTableBuilder( ENTITY_ID_LOOKUP )
                        .ifNotExists()
                        .partitionKey( CommonColumns.SYNCID, CommonColumns.ENTITY_SET_ID )
                        .clusteringColumns( CommonColumns.ENTITYID )
                        .secondaryIndex( CommonColumns.ENTITY_SET_ID );
            case DATA:
                return new CassandraTableBuilder( DATA )
                        .ifNotExists()
                        .partitionKey( CommonColumns.ENTITYID )
                        .clusteringColumns( CommonColumns.SYNCID,
                                CommonColumns.PROPERTY_TYPE_ID,
                                CommonColumns.PROPERTY_VALUE );
            case ENTITY_SETS:
                return new CassandraTableBuilder( ENTITY_SETS )
                        .ifNotExists()
                        .partitionKey( CommonColumns.ID )
                        .clusteringColumns( CommonColumns.NAME )
                        .columns( CommonColumns.ENTITY_TYPE_ID,
                                CommonColumns.TITLE,
                                CommonColumns.DESCRIPTION )
                        .secondaryIndex( CommonColumns.ENTITY_TYPE_ID, CommonColumns.NAME );
            case ENTITY_TYPES:
                return new CassandraTableBuilder( ENTITY_TYPES )
                        .ifNotExists()
                        .partitionKey( CommonColumns.ID )
                        .clusteringColumns( CommonColumns.NAMESPACE, CommonColumns.NAME )
                        .columns( CommonColumns.TITLE,
                                CommonColumns.DESCRIPTION,
                                CommonColumns.KEY,
                                CommonColumns.PROPERTIES,
                                CommonColumns.SCHEMAS )
                        .secondaryIndex( CommonColumns.NAMESPACE, CommonColumns.SCHEMAS );
            case NAMES:
                return new CassandraTableBuilder( NAMES )
                        .ifNotExists()
                        .partitionKey( CommonColumns.SECURABLE_OBJECTID )
                        .columns( CommonColumns.NAME );
            case ORGANIZATIONS:
                return new CassandraTableBuilder( ORGANIZATIONS )
                        .ifNotExists()
                        .partitionKey( CommonColumns.ID )
                        .columns( CommonColumns.TITLE,
                                CommonColumns.DESCRIPTION,
                                CommonColumns.TRUSTED_ORGANIZATIONS,
                                CommonColumns.ALLOWED_EMAIL_DOMAINS,
                                CommonColumns.MEMBERS,
                                CommonColumns.ROLES );
            case PROPERTY_TYPES:
                return new CassandraTableBuilder( PROPERTY_TYPES )
                        .ifNotExists()
                        .partitionKey( CommonColumns.ID )
                        .clusteringColumns( CommonColumns.NAMESPACE, CommonColumns.NAME )
                        .columns( CommonColumns.TITLE,
                                CommonColumns.DESCRIPTION,
                                CommonColumns.SCHEMAS,
                                CommonColumns.DATATYPE )
                        .secondaryIndex( CommonColumns.NAMESPACE, CommonColumns.SCHEMAS );
            case PERMISSIONS:
                // TODO: Once Cassandra fixes SASI + Collection column inde
                return new CassandraTableBuilder( PERMISSIONS )
                        .ifNotExists()
                        .partitionKey( CommonColumns.ACL_KEYS )
                        .clusteringColumns( CommonColumns.PRINCIPAL_TYPE, CommonColumns.PRINCIPAL_ID )
                        .columns( CommonColumns.PERMISSIONS )
                        .staticColumns( CommonColumns.SECURABLE_OBJECT_TYPE )
                        .secondaryIndex( CommonColumns.PERMISSIONS, CommonColumns.SECURABLE_OBJECT_TYPE );
            case PERMISSIONS_REQUESTS_UNRESOLVED:
                return new CassandraTableBuilder( PERMISSIONS_REQUESTS_UNRESOLVED )
                        .ifNotExists()
                        .partitionKey( CommonColumns.ACL_ROOT )
                        .clusteringColumns( CommonColumns.PRINCIPAL_ID )
                        .columns( CommonColumns.ACL_CHILDREN_PERMISSIONS, CommonColumns.STATUS )
                        .sasi( CommonColumns.STATUS );
            case PERMISSIONS_REQUESTS_RESOLVED:
                return new CassandraTableBuilder( PERMISSIONS_REQUESTS_RESOLVED )
                        .ifNotExists()
                        .partitionKey( CommonColumns.PRINCIPAL_ID )
                        .clusteringColumns( CommonColumns.REQUESTID )
                        .columns( CommonColumns.ACL_ROOT, CommonColumns.ACL_CHILDREN_PERMISSIONS, CommonColumns.STATUS )
                        .fullCollectionIndex( CommonColumns.ACL_ROOT )
                        .sasi( CommonColumns.STATUS );
            case REQUESTS:
                return new CassandraTableBuilder( REQUESTS )
                        .ifNotExists()
                        .partitionKey( CommonColumns.ACL_KEYS )
                        .clusteringColumns( CommonColumns.PRINCIPAL_TYPE, CommonColumns.PRINCIPAL_ID )
                        .columns( CommonColumns.PERMISSIONS, CommonColumns.STATUS )
                        .sasi( CommonColumns.PRINCIPAL_TYPE,
                                CommonColumns.PRINCIPAL_ID,
                                CommonColumns.STATUS );
            case SCHEMAS:
                return new CassandraTableBuilder( SCHEMAS )
                        .ifNotExists()
                        .partitionKey( CommonColumns.NAMESPACE )
                        .columns( CommonColumns.NAME_SET );
            default:
                logger.error( "Missing table configuration {}, unable to start.", table.name() );
                throw new IllegalStateException( "Missing table configuration " + table.name() + ", unable to start." );
        }
    }

    public String getName() {
        return name();
    }

    public String getKeyspace() {
        return keyspace;
    }

    public CassandraTableBuilder getBuilder() {
        return getTableDefinition( this );
    }

    public TableDef asTableDef() {
        return this;
    }

}
