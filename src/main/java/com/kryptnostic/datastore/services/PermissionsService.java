package com.kryptnostic.datastore.services;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.kryptnostic.conductor.rpc.odata.EntityType;
import com.kryptnostic.datastore.Permission;

public class PermissionsService implements PermissionsManager {

    private final Session               session;
    private final Mapper<EntityType>    entityTypeMapper;
    private final CassandraTableManager tableManager;
    private final CassandraEdmStore     edmStore;

    public PermissionsService( Session session, MappingManager mappingManager, CassandraTableManager tableManager ) {
        this.session = session;
        this.tableManager = tableManager;
        this.entityTypeMapper = mappingManager.mapper( EntityType.class );
        this.edmStore = mappingManager.createAccessor( CassandraEdmStore.class );
    }

    @Override
    public void addPermissionsForEntityType( String role, FullQualifiedName fqn, Set<Permission> permissions ) {
        tableManager.addPermissionsForEntityType( role, fqn, permissions );
    }

    @Override
    public void removePermissionsForEntityType( String role, FullQualifiedName fqn, Set<Permission> permissions ) {
        EnumSet<Permission> userPermissions = getPermissionsForEntityType( role, fqn );
        userPermissions.removeAll( permissions );

        setPermissionsForEntityType( role, fqn, userPermissions );
    }

    @Override
    public void setPermissionsForEntityType( String role, FullQualifiedName fqn, Set<Permission> permissions ) {
        if( !permissions.isEmpty() ){
            tableManager.setPermissionsForEntityType( role, fqn, permissions );
        } else {
            tableManager.deleteFromEntityTypesAclsTable( role, fqn );
        }
    }

    @Override
    public boolean checkUserHasPermissionsOnEntityType(
            List<String> roles,
            FullQualifiedName fqn,
            Permission permission ) {
        EnumSet<Permission> userPermissions = getPermissionsForEntityType( roles, fqn );

        return userPermissions.contains( permission );
    }

    private EnumSet<Permission> getPermissionsForEntityType( String role, FullQualifiedName fqn ) {
        return tableManager.getPermissionsForEntityType( role, fqn );
    }

    private EnumSet<Permission> getPermissionsForEntityType( List<String> roles, FullQualifiedName fqn ) {
        return roles.stream()
                .flatMap( role -> getPermissionsForEntityType( role, fqn ).stream() )
                .collect(Collectors.toCollection(() -> EnumSet.noneOf(Permission.class)));
    }

    @Override
    public void addPermissionsForEntitySet(
            String role,
            FullQualifiedName type,
            String name,
            Set<Permission> permissions ) {
        tableManager.addPermissionsForEntitySet( role, type, name, permissions );
    }

    @Override
    public void removePermissionsForEntitySet(
            String role,
            FullQualifiedName type,
            String name,
            Set<Permission> permissions ) {
        EnumSet<Permission> userPermissions = getPermissionsForEntitySet( role, type, name );
        userPermissions.removeAll( permissions );

        setPermissionsForEntitySet( role, type, name, userPermissions );
    }

    @Override
    public void setPermissionsForEntitySet(
            String role,
            FullQualifiedName type,
            String name,
            Set<Permission> permissions ) {
        if( !permissions.isEmpty() ){
            tableManager.setPermissionsForEntitySet( role, type, name, permissions );
        } else {
            tableManager.deleteFromEntitySetsAclsTable( role, type, name );
        }
    }

    @Override
    public boolean checkUserHasPermissionsOnEntitySet(
            List<String> roles,
            FullQualifiedName type,
            String name,
            Permission permission ) {
        EnumSet<Permission> userPermissions = getPermissionsForEntitySet( roles, type, name );

        return userPermissions.contains( permission );
    }

    private EnumSet<Permission> getPermissionsForEntitySet( String role, FullQualifiedName type, String name ) {
        return tableManager.getPermissionsForEntitySet( role, type, name );
    }
    
    private EnumSet<Permission> getPermissionsForEntitySet( List<String> roles, FullQualifiedName type, String name ) {
        return roles.stream()
                .flatMap( role -> getPermissionsForEntitySet( role, type, name ).stream() )
                .collect(Collectors.toCollection(() -> EnumSet.noneOf(Permission.class)));
    }

    @Override
    public void addPermissionsForPropertyTypeInEntityType(
            String role,
            FullQualifiedName entityTypeFqn,
            FullQualifiedName propertyTypeFqn,
            Set<Permission> permissions ) {
        tableManager.addPermissionsForPropertyTypeInEntityType( role,
                entityTypeFqn,
                propertyTypeFqn,
                permissions );
    }

    @Override
    public void removePermissionsForPropertyTypeInEntityType(
            String role,
            FullQualifiedName entityTypeFqn,
            FullQualifiedName propertyTypeFqn,
            Set<Permission> permissions ) {
        EnumSet<Permission> userPermissions = getPermissionsForPropertyTypeInEntityType( role, entityTypeFqn, propertyTypeFqn );
        userPermissions.removeAll( permissions );
        
        setPermissionsForPropertyTypeInEntityType( role, entityTypeFqn, propertyTypeFqn, userPermissions);
    }

    @Override
    public void setPermissionsForPropertyTypeInEntityType(
            String role,
            FullQualifiedName entityTypeFqn,
            FullQualifiedName propertyTypeFqn,
            Set<Permission> permissions ) {
        if( !permissions.isEmpty() ){
            tableManager.setPermissionsForPropertyTypeInEntityType( role, entityTypeFqn, propertyTypeFqn, permissions );
        } else {
            tableManager.deleteFromPropertyTypesInEntityTypesAclsTable( role, entityTypeFqn, propertyTypeFqn );
        }
    }

    @Override
    public boolean checkUserHasPermissionsOnPropertyTypeInEntityType(
            List<String> roles,
            FullQualifiedName entityTypeFqn,
            FullQualifiedName propertyTypeFqn,
            Permission permission ) {
        EnumSet<Permission> userPermissions = getPermissionsForPropertyTypeInEntityType( roles, entityTypeFqn, propertyTypeFqn );

        return userPermissions.contains( permission );
    }

    private EnumSet<Permission> getPermissionsForPropertyTypeInEntityType(
            String role,
            FullQualifiedName entityTypeFqn,
            FullQualifiedName propertyTypeFqn ) {
        return tableManager.getPermissionsForPropertyTypeInEntityType( role, entityTypeFqn, propertyTypeFqn );
    }

    private EnumSet<Permission> getPermissionsForPropertyTypeInEntityType(
            List<String> roles,
            FullQualifiedName entityTypeFqn,
            FullQualifiedName propertyTypeFqn ) {
        return roles.stream()
                .flatMap( role -> getPermissionsForPropertyTypeInEntityType( role, entityTypeFqn, propertyTypeFqn ).stream() )
                .collect(Collectors.toCollection(() -> EnumSet.noneOf(Permission.class)));
    }

    @Override
    public void addPermissionsForPropertyTypeInEntitySet(
            String role,
            FullQualifiedName entityTypeFqn,
            String entitySetName,
            FullQualifiedName propertyTypeFqn,
            Set<Permission> permissions ) {
        tableManager.addPermissionsForPropertyTypeInEntitySet( role,
                entityTypeFqn,
                entitySetName,
                propertyTypeFqn,
                permissions );
    }

    @Override
    public void removePermissionsForPropertyTypeInEntitySet(
            String role,
            FullQualifiedName entityTypeFqn,
            String entitySetName,
            FullQualifiedName propertyTypeFqn,
            Set<Permission> permissions ) {
        EnumSet<Permission> userPermissions = getPermissionsForPropertyTypeInEntitySet( role,
                entityTypeFqn,
                entitySetName,
                propertyTypeFqn );
        userPermissions.removeAll( permissions );

        setPermissionsForPropertyTypeInEntitySet( role,
                entityTypeFqn,
                entitySetName,
                propertyTypeFqn,
                userPermissions );
    }

    @Override
    public void setPermissionsForPropertyTypeInEntitySet(
            String role,
            FullQualifiedName entityTypeFqn,
            String entitySetName,
            FullQualifiedName propertyTypeFqn,
            Set<Permission> permissions ) {
        if( !permissions.isEmpty() ){
            tableManager.setPermissionsForPropertyTypeInEntitySet( role,
                    entityTypeFqn,
                    entitySetName,
                    propertyTypeFqn,
                    permissions );
        } else {
            tableManager.deleteFromPropertyTypesInEntitySetsAclsTable( role,
                    entityTypeFqn,
                    entitySetName,
                    propertyTypeFqn );
        }
    }

    @Override
    public boolean checkUserHasPermissionsOnPropertyTypeInEntitySet(
            List<String> roles,
            FullQualifiedName entityTypeFqn,
            String entitySetName,
            FullQualifiedName propertyTypeFqn,
            Permission permission ) {
        EnumSet<Permission> userPermissions = getPermissionsForPropertyTypeInEntitySet( roles,
                entityTypeFqn,
                entitySetName,
                propertyTypeFqn );
        
        return userPermissions.contains( permission );
    }

    private EnumSet<Permission> getPermissionsForPropertyTypeInEntitySet(
            String role,
            FullQualifiedName entityTypeFqn,
            String entitySetName,
            FullQualifiedName propertyTypeFqn ) {
        return tableManager.getPermissionsForPropertyTypeInEntitySet( role,
                entityTypeFqn,
                entitySetName,
                propertyTypeFqn );
    }

    private EnumSet<Permission> getPermissionsForPropertyTypeInEntitySet(
            List<String> roles,
            FullQualifiedName entityTypeFqn,
            String entitySetName,
            FullQualifiedName propertyTypeFqn ) {
        return roles.stream()
                .flatMap( role -> getPermissionsForPropertyTypeInEntitySet( role,
                        entityTypeFqn,
                        entitySetName,
                        propertyTypeFqn ).stream() )
                .collect(Collectors.toCollection(() -> EnumSet.noneOf(Permission.class)));
    }

    @Override
    public void removePermissionsForEntityType( FullQualifiedName fqn ) {
        tableManager.deleteFromEntityTypesAclsTable( fqn );
    }

    @Override
    public void removePermissionsForEntitySet( FullQualifiedName entityTypeName, String entitySetName ) {
        tableManager.deleteFromEntitySetsAclsTable( entityTypeName, entitySetName );
    }

    @Override
    public void removePermissionsForPropertyTypeInEntityType(
            FullQualifiedName entityTypeFqn,
            FullQualifiedName propertyTypeFqn ) {
        tableManager.deleteFromPropertyTypesInEntityTypesAclsTable( entityTypeFqn, propertyTypeFqn );
    }

    @Override
    public void removePermissionsForPropertyTypeInEntityType( FullQualifiedName entityTypeFqn ) {
        tableManager.deleteFromPropertyTypesInEntityTypesAclsTable( entityTypeFqn );
    }

    @Override
    public void removePermissionsForPropertyTypeInEntitySet(
            FullQualifiedName entityTypeFqn,
            String entitySetName,
            FullQualifiedName propertyTypeFqn ) {
        tableManager.deleteFromPropertyTypesInEntitySetsAclsTable( entityTypeFqn, entitySetName, propertyTypeFqn );
    }

    @Override
    public void removePermissionsForPropertyTypeInEntitySet( FullQualifiedName entityTypeFqn, String entitySetName ) {
        tableManager.deleteFromPropertyTypesInEntitySetsAclsTable( entityTypeFqn, entitySetName );
    }

}
