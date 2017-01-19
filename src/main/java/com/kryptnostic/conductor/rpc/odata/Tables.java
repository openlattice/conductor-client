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
                        .columns( CommonColumns.SECURABLE_OBJECTID );
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
            case FQNS:
                return new CassandraTableBuilder( FQNS )
                        .ifNotExists()
                        .partitionKey( CommonColumns.SECURABLE_OBJECTID )
                        .columns( CommonColumns.FQN );
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

}
