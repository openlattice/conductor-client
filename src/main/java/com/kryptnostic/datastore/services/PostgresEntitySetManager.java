package com.kryptnostic.datastore.services;

import com.dataloom.edm.EntitySet;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.openlattice.postgres.PostgresColumn;
import com.openlattice.postgres.PostgresTable;
import com.openlattice.postgres.ResultSetAdapters;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;

public class PostgresEntitySetManager {
    private static final Logger logger = LoggerFactory.getLogger( PostgresEntitySetManager.class );
    private final HikariDataSource hds;

    private final String getAllEntitySets;
    private final String getEntitySet;
    private final String getEntitySetsByType;

    public PostgresEntitySetManager( HikariDataSource hds ) {
        this.hds = hds;

        // Tables
        String ENTITY_SETS = PostgresTable.ENTITY_SETS.getName();

        // Properties
        String NAME = PostgresColumn.NAME.getName();
        String ENTITY_TYPE_ID = PostgresColumn.ENTITY_TYPE_ID.getName();

        // SQL queries
        this.getAllEntitySets = "SELECT * FROM ".concat( ENTITY_SETS ).concat( ";" );
        this.getEntitySet = "SELECT * FROM ".concat( ENTITY_SETS ).concat( " WHERE " ).concat( NAME )
                .concat( " = ?;" );
        this.getEntitySetsByType = "SELECT * FROM ".concat( ENTITY_SETS ).concat( " WHERE " ).concat( ENTITY_TYPE_ID )
                .concat( " = ?;" );
    }

    public EntitySet getEntitySet( String entitySetName ) {
        try {
            Connection connection = hds.getConnection();
            PreparedStatement ps = connection.prepareStatement( getEntitySet );
            ps.setString( 1, entitySetName );
            ResultSet rs = ps.executeQuery();
            rs.next();
            EntitySet entitySet = ResultSetAdapters.entitySet( rs );
            connection.close();
            return entitySet;
        } catch ( SQLException e ) {
            logger.debug( "Unable to load entity set {}", entitySetName, e );
            return null;
        }
    }

    public Iterable<EntitySet> getAllEntitySets() {
        try {
            List<EntitySet> result = Lists.newArrayList();

            Connection connection = hds.getConnection();
            PreparedStatement ps = connection.prepareStatement( getAllEntitySets );

            ResultSet rs = ps.executeQuery();
            while ( rs.next() ) {
                result.add( ResultSetAdapters.entitySet( rs ) );
            }

            connection.close();
            return result;
        } catch ( SQLException e ) {
            logger.debug( "Unable to load all entity sets", e );
            return ImmutableList.of();
        }
    }

    public Iterable<EntitySet> getAllEntitySetsForType( UUID entityTypeId ) {
        try {
            List<EntitySet> result = Lists.newArrayList();

            Connection connection = hds.getConnection();
            PreparedStatement ps = connection.prepareStatement( getEntitySetsByType );
            ps.setObject( 1, entityTypeId );

            ResultSet rs = ps.executeQuery();
            while ( rs.next() ) {
                result.add( ResultSetAdapters.entitySet( rs ) );
            }

            connection.close();
            return result;
        } catch ( SQLException e ) {
            logger.debug( "Unable to load entity sets for entity type id {}", entityTypeId.toString(), e );
            return ImmutableList.of();
        }
    }

}
