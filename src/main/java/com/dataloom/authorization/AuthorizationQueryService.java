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

package com.dataloom.authorization;

import com.dataloom.authorization.paging.AuthorizedObjectsSearchResult;
import com.dataloom.authorization.securable.SecurableObjectType;
import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.streams.StreamUtil;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.openlattice.postgres.*;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class AuthorizationQueryService {
    private static final Logger logger = LoggerFactory
            .getLogger( AuthorizationQueryService.class );
    private final HikariDataSource                         hds;
    private final IMap<AceKey, DelegatedPermissionEnumSet> aces;

    private final String aclsForSecurableObjectSql;
    private final String ownersForSecurableObjectSql;
    private final String deletePermissionsByAclKeysSql;
    private final String setObjectTypeSql;

    public AuthorizationQueryService( HikariDataSource hds, HazelcastInstance hazelcastInstance ) {
        this.hds = hds;
        aces = hazelcastInstance.getMap( HazelcastMap.PERMISSIONS.name() );

        // Tables
        String PERMISSIONS_TABLE = PostgresTable.PERMISSIONS.getName();
        String SECURABLE_OBJECTS = PostgresTable.SECURABLE_OBJECTS.getName();

        // Columns
        String ACL_KEY = PostgresColumn.ACL_KEY.getName();
        String PRINCIPAL_TYPE = PostgresColumn.PRINCIPAL_TYPE.getName();
        String PRINCIPAL_ID = PostgresColumn.PRINCIPAL_ID.getName();
        String PERMISSIONS = PostgresColumn.PERMISSIONS.getName();
        String SECURABLE_OBJECT_TYPE = PostgresColumn.SECURABLE_OBJECT_TYPE.getName();

        this.aclsForSecurableObjectSql = PostgresQuery
                .selectColsFrom( PERMISSIONS_TABLE, ImmutableList.of( PRINCIPAL_TYPE, PRINCIPAL_ID ) )
                .concat( PostgresQuery.whereEq( ImmutableList.of( ACL_KEY ), true ) );
        this.ownersForSecurableObjectSql = PostgresQuery
                .selectColsFrom( PERMISSIONS_TABLE, ImmutableList.of( PRINCIPAL_TYPE, PRINCIPAL_ID ) )
                .concat( PostgresQuery.whereEq( ImmutableList.of( ACL_KEY ) ) )
                .concat( PostgresQuery.AND ).concat( PostgresQuery.valueInArray( PERMISSIONS, true ) );
        this.deletePermissionsByAclKeysSql = PostgresQuery.deleteFrom( PERMISSIONS_TABLE )
                .concat( PostgresQuery.whereEq( ImmutableList.of( ACL_KEY ), true ) );
        this.setObjectTypeSql = PostgresQuery.update( SECURABLE_OBJECTS )
                .concat( PostgresQuery.setEq( ImmutableList.of( SECURABLE_OBJECT_TYPE ) ) )
                .concat( PostgresQuery.whereEq( ImmutableList.of( ACL_KEY ), true ) );
    }

    private String getAuthorizedAclKeyQuery(
            boolean securableObjectType,
            int numPrincipals,
            boolean limit,
            boolean offset ) {
        // Tables
        String PERMISSIONS_TABLE = PostgresTable.PERMISSIONS.getName();
        String SECURABLE_OBJECTS = PostgresTable.SECURABLE_OBJECTS.getName();

        // Columns
        String ACL_KEY = PostgresColumn.ACL_KEY.getName();
        String PRINCIPAL_TYPE = PostgresColumn.PRINCIPAL_TYPE.getName();
        String PRINCIPAL_ID = PostgresColumn.PRINCIPAL_ID.getName();
        String PERMISSIONS = PostgresColumn.PERMISSIONS.getName();
        String SECURABLE_OBJECT_TYPE = PostgresColumn.SECURABLE_OBJECT_TYPE.getName();
        String PT_ACL_KEY = PERMISSIONS_TABLE.concat( "." ).concat( ACL_KEY );
        String SO_ACL_KEY = SECURABLE_OBJECTS.concat( "." ).concat( ACL_KEY );

        // SELECT permissions.acl_key from permissions, securable_objects
        StringBuilder sql = new StringBuilder( PostgresQuery
                .selectColsFrom( ImmutableList.of( PERMISSIONS_TABLE, SECURABLE_OBJECTS ),
                        ImmutableList.of( PT_ACL_KEY ) ) );

        // WHERE permissions.acl_key = securable_objects.acl_key
        sql.append( PostgresQuery.whereColsAreEq( PT_ACL_KEY, SO_ACL_KEY ) );

        // AND [<SomePermission>, <AnotherPermission>] @< permissions
        sql.append( PostgresQuery.AND ).append( PostgresQuery.valuesInArray( PERMISSIONS ) );

        // maybe: AND securable_object_type = ?
        if ( securableObjectType )
            sql.append( PostgresQuery.AND ).append( PostgresQuery.eq( SECURABLE_OBJECT_TYPE ) );

        // AND ( (principal_type = ? AND principal_id = ?) OR (principal_type = ? AND principal_id = ?) OR ... )
        String singlePrincipalCheck = "(".concat( PostgresQuery.eq( PRINCIPAL_TYPE ) ).concat( PostgresQuery.AND )
                .concat( PostgresQuery.eq( PRINCIPAL_ID ) ).concat( ")" );
        sql.append( PostgresQuery.AND ).append( "(" )
                .append( Stream.generate( () -> singlePrincipalCheck ).limit( numPrincipals )
                        .collect( Collectors.joining( PostgresQuery.OR ) ) ).append( ")" );

        // maybe: LIMIT ?
        if ( limit )
            sql.append( PostgresQuery.LIMIT );

        // maybe: OFFSET ?
        if ( offset )
            sql.append( PostgresQuery.OFFSET );

        sql.append( PostgresQuery.END );
        return sql.toString();
    }

    /**
     * get all authorized acl keys for a principal, of a fixed object type, with desired permissions.
     *
     * @param principal
     * @param objectType
     * @param desiredPermissions
     * @return
     */
    public Stream<List<UUID>> getAuthorizedAclKeys(
            Principal principal,
            SecurableObjectType objectType,
            EnumSet<Permission> desiredPermissions ) {
        return getAuthorizedAclKeysForPrincipals( ImmutableSet.of( principal ),
                desiredPermissions,
                Optional.of( objectType ) );
    }

    /**
     * get all authorized acl keys for a set of principals, of a fixed object type, with desired permissions.
     *
     * @param principals
     * @param objectType
     * @param desiredPermissions
     * @return
     */
    public Set<List<UUID>> getAuthorizedAclKeys(
            Set<Principal> principals,
            SecurableObjectType objectType,
            EnumSet<Permission> desiredPermissions ) {
        return getAuthorizedAclKeysForPrincipals( principals, desiredPermissions, Optional.of( objectType ) )
                .collect( Collectors.toSet() );
    }

    /**
     * get all authorized acl keys for a set of principals, of a fixed object type, with specified permission, starting from a page given from paging state.
     */
    public AuthorizedObjectsSearchResult getAuthorizedAclKeys(
            NavigableSet<Principal> principals,
            SecurableObjectType objectType,
            Permission permission,
            String offsetStr,
            int pageSize ) {

        int limit = pageSize;
        int offset = ( offsetStr == null ) ? 0 : Integer.parseInt( offsetStr );

        Set<List<UUID>> results = getAuthorizedAclKeysForPrincipals( principals,
                EnumSet.of( permission ),
                Optional.of( objectType ),
                Optional.of( limit ),
                Optional.of( offset ) )
                .collect( Collectors.toSet() );

        String newPage = ( results.size() < pageSize ) ? null : String.valueOf( offset + pageSize );

        return new AuthorizedObjectsSearchResult( newPage, results );
    }

    /**
     * get all authorized acl keys for a principal, of all object types, with desired permissions.
     *
     * @param principal
     * @param desiredPermissions
     * @return
     */
    public Stream<List<UUID>> getAuthorizedAclKeys(
            Principal principal,
            EnumSet<Permission> desiredPermissions ) {
        return getAuthorizedAclKeysForPrincipals( ImmutableSet.of( principal ), desiredPermissions, Optional.empty() );
    }

    /**
     * get all authorized acl keys for a set of principals, of all object types, with desired permissions.
     *
     * @param principals
     * @param desiredPermissions
     * @return
     */
    public Stream<List<UUID>> getAuthorizedAclKeys(
            Set<Principal> principals,
            EnumSet<Permission> desiredPermissions ) {
        return getAuthorizedAclKeysForPrincipals( principals, desiredPermissions, Optional.empty() );
    }

    private PreparedStatement prepareAuthorizedAclKeysQuery(
            Connection connection,
            Set<Principal> principals,
            EnumSet<Permission> permissions,
            Optional<SecurableObjectType> securableObjectType,
            Optional<Integer> limit,
            Optional<Integer> offset ) throws SQLException {
        String sql = getAuthorizedAclKeyQuery( securableObjectType.isPresent(),
                principals.size(),
                limit.isPresent(),
                offset.isPresent() );
        PreparedStatement ps = connection.prepareStatement( sql );
        int count = 1;

        ps.setArray( count, PostgresArrays
                .createTextArray( connection, permissions.stream().map( permission -> permission.name() ) ) );
        count++;

        if ( securableObjectType.isPresent() ) {
            ps.setString( count, securableObjectType.get().name() );
            count++;
        }

        for ( Principal principal : principals ) {
            ps.setString( count, principal.getType().name() );
            count++;
            ps.setString( count, principal.getId() );
            count++;
        }

        if ( limit.isPresent() ) {
            ps.setInt( count, limit.get() );
            count++;
        }

        if ( offset.isPresent() ) {
            ps.setInt( count, offset.get() );
        }

        return ps;
    }

    public Stream<List<UUID>> getAuthorizedAclKeysForPrincipals(
            Set<Principal> principals,
            EnumSet<Permission> desiredPermissions,
            Optional<SecurableObjectType> securableObjectType ) {
        return getAuthorizedAclKeysForPrincipals( principals,
                desiredPermissions,
                securableObjectType,
                Optional.empty(),
                Optional.empty() );
    }

    public Stream<List<UUID>> getAuthorizedAclKeysForPrincipals(
            Set<Principal> principals,
            EnumSet<Permission> desiredPermissions,
            Optional<SecurableObjectType> securableObjectType,
            Optional<Integer> limit,
            Optional<Integer> offset ) {
        try {
            List<List<UUID>> result = Lists.newArrayList();
            Connection connection = hds.getConnection();
            PreparedStatement ps = prepareAuthorizedAclKeysQuery( connection,
                    principals,
                    desiredPermissions,
                    securableObjectType,
                    limit,
                    offset );
            ResultSet rs = ps.executeQuery();
            while ( rs.next() ) {
                result.add( ResultSetAdapters.aclKey( rs ) );
            }
            connection.close();
            return StreamUtil.stream( result );
        } catch ( SQLException e ) {
            logger.debug( "Unable to get authorized acl keys.", e );
            return Stream.empty();
        }
    }

    public Stream<Principal> getPrincipalsForSecurableObject( List<UUID> aclKeys ) {
        try {
            List<Principal> result = Lists.newArrayList();
            Connection connection = hds.getConnection();
            PreparedStatement ps = connection.prepareStatement( aclsForSecurableObjectSql );
            ps.setArray( 1, PostgresArrays.createUuidArray( connection, aclKeys.stream() ) );

            ResultSet rs = ps.executeQuery();
            while ( rs.next() ) {
                result.add( ResultSetAdapters.principal( rs ) );
            }
            connection.close();
            return StreamUtil.stream( result );
        } catch ( SQLException e ) {
            logger.debug( "Unable to get principals for object {}.", aclKeys, e );
            return Stream.empty();
        }
    }

    public Acl getAclsForSecurableObject( List<UUID> aclKeys ) {
        Stream<Ace> accessControlEntries = getPrincipalsForSecurableObject( aclKeys )
                .map( principal -> new AceKey( aclKeys, principal ) )
                .map( aceKey -> new AceFuture( aceKey.getPrincipal(), aces.getAsync( aceKey ) ) )
                .map( AceFuture::getUninterruptibly );
        return new Acl( aclKeys, accessControlEntries::iterator );

    }

    public void createEmptyAcl( List<UUID> aclKey, SecurableObjectType objectType ) {
        try {
            Connection connection = hds.getConnection();
            PreparedStatement ps = connection.prepareStatement( setObjectTypeSql );
            ps.setString( 1, objectType.name() );
            ps.setArray( 2, PostgresArrays.createUuidArray( connection, aclKey.stream() ) );
            ps.execute();
            connection.close();
            logger.info( "Created empty acl with key {} and type {}", aclKey, objectType );
        } catch ( SQLException e ) {
            logger.debug( "Unable to create acl with key {} and type {}", aclKey, objectType, e );
        }
    }

    public void deletePermissionsByAclKeys( List<UUID> aclKey ) {
        try {
            Connection connection = hds.getConnection();
            PreparedStatement ps = connection.prepareStatement( deletePermissionsByAclKeysSql );
            ps.setArray( 1, PostgresArrays.createUuidArray( connection, aclKey.stream() ) );
            ps.execute();
            connection.close();
            logger.info( "Deleted all permissions for aclKey {}", aclKey );
        } catch ( SQLException e ) {
            logger.debug( "Unable delete all permissions for aclKey {}.", aclKey, e );
        }
    }

    public Iterable<Principal> getOwnersForSecurableObject( List<UUID> aclKeys ) {
        try {
            List<Principal> result = Lists.newArrayList();
            Connection connection = hds.getConnection();
            PreparedStatement ps = connection.prepareStatement( ownersForSecurableObjectSql );
            ps.setArray( 1, PostgresArrays.createUuidArray( connection, aclKeys.stream() ) );
            ps.setArray( 2, PostgresArrays.createTextArray( connection, Stream.of( Permission.OWNER.name() ) ) );

            ResultSet rs = ps.executeQuery();
            while ( rs.next() ) {
                result.add( ResultSetAdapters.principal( rs ) );
            }
            connection.close();
            return result;
        } catch ( SQLException e ) {
            logger.debug( "Unable to get owners for securable object {}.", aclKeys, e );
            return ImmutableList.of();
        }
    }
}
