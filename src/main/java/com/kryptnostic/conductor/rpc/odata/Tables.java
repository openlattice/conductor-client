package com.kryptnostic.conductor.rpc.odata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataloom.edm.internal.DatastoreConstants;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.datastore.util.Util;
import com.kryptnostic.rhizome.cassandra.CassandraTableBuilder;
import com.kryptnostic.rhizome.cassandra.TableDef;

public enum Tables implements TableDef {
    ACL_KEYS,
    DATA,
    ENTITY_ID_LOOKUP,
    ENTITY_SETS,
    ENTITY_TYPES,
    FQNS,
    ORGANIZATIONS,
    PERMISSIONS,
    PERMISSIONS_REQUESTS_UNRESOLVED,
    PERMISSIONS_REQUESTS_RESOLVED,
    PROPERTY_TYPES,
    SCHEMAS,
    ;

    private static final Logger logger = LoggerFactory.getLogger( Tables.class );

    public String getName() {
        return name();
    }

    public String getKeyspace() {
        CassandraTableBuilder builder = getBuilder();
        return builder == null ? Util.getSafely( TablesHelper.keyspaces, this )
                : builder.getKeyspace().or( DatastoreConstants.KEYSPACE );
    }

    public CassandraTableBuilder getBuilder() {
        return Util.getSafely( TablesHelper.builders, this );
    }

    public TableDef asTableDef() {
        return this;
    }

    static CassandraTableBuilder getTableDefinition( Tables table ) {
        switch ( table ) {
            case ACL_KEYS:
                return new CassandraTableBuilder( ACL_KEYS )
                        .ifNotExists()
                        .partitionKey( CommonColumns.FQN )
                        .columns( CommonColumns.SECURABLE_OBJECT_TYPE, CommonColumns.SECURABLE_OBJECTID );
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
                        .columns( CommonColumns.TYPE,
                                CommonColumns.ENTITY_TYPE_ID,
                                CommonColumns.TITLE,
                                CommonColumns.DESCRIPTION )
                        .secondaryIndex( CommonColumns.TYPE, CommonColumns.NAME );
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
            case FQNS:
                return new CassandraTableBuilder( FQNS )
                        .ifNotExists()
                        .partitionKey( CommonColumns.SECURABLE_OBJECT_TYPE, CommonColumns.SECURABLE_OBJECTID )
                        .columns( CommonColumns.FQN );
            case ORGANIZATIONS:
                return new CassandraTableBuilder( ORGANIZATIONS )
                        .ifNotExists()
                        .partitionKey( CommonColumns.ID )
                        .clusteringColumns( CommonColumns.NAMESPACE, CommonColumns.NAME )
                        .columns( CommonColumns.TITLE,
                                CommonColumns.DESCRIPTION,
                                CommonColumns.SCHEMAS );
            case PROPERTY_TYPES:
                return new CassandraTableBuilder( PROPERTY_TYPES )
                        .ifNotExists()
                        .partitionKey( CommonColumns.ID )
                        .clusteringColumns( CommonColumns.NAMESPACE, CommonColumns.NAME )
                        .columns( CommonColumns.TITLE,
                                CommonColumns.DESCRIPTION,
                                CommonColumns.SCHEMAS )
                        .secondaryIndex( CommonColumns.NAMESPACE, CommonColumns.SCHEMAS );
            case PERMISSIONS:
                return new CassandraTableBuilder( PERMISSIONS )
                        .ifNotExists()
                        .partitionKey( CommonColumns.ACL_KEYS )
                        .clusteringColumns( CommonColumns.PRINCIPAL_TYPE, CommonColumns.PRINCIPAL_ID )
                        .columns( CommonColumns.SECURABLE_OBJECT_TYPE, CommonColumns.PERMISSIONS )
                        .secondaryIndex( CommonColumns.PERMISSIONS )
                        .sasi( CommonColumns.SECURABLE_OBJECT_TYPE );
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
                        .secondaryIndex( CommonColumns.ACL_ROOT )
                        .sasi( CommonColumns.STATUS );
            case SCHEMAS:
                return new CassandraTableBuilder( SCHEMAS )
                        .ifNotExists()
                        .partitionKey( CommonColumns.NAMESPACE )
                        .clusteringColumns( CommonColumns.NAME );
            default:
                logger.error( "Missing table configuration {}, unable to start.", table.name() );
                throw new IllegalStateException( "Missing table configuration " + table.name() + ", unable to start." );
        }
    }

}
