package com.kryptnostic.datastore.services;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.springframework.security.core.context.SecurityContextHolder;

import com.kryptnostic.datastore.Permission;

public class ActionAuthorizationService {

//    private List<String> currentRoles = SecurityContextHolder.getContext().getAuthentication().getAuthorities().stream().map(grantedAuthority -> grantedAuthority.getAuthority()).collect( Collectors.toList() );
    private List<String> currentRoles;
    
    private final PermissionsService ps;

    public ActionAuthorizationService( PermissionsService ps ) {
        this.ps = ps;
    };

    /**
     * Entity Data Model actions
     */

    public boolean upsertPropertyType( FullQualifiedName propertyTypeFqn ) {
        return true;
    }

    public boolean upsertEntityType( FullQualifiedName entityTypeFqn ) {
        return ps.checkUserHasPermissionsOnEntityType( currentRoles, entityTypeFqn, Permission.ALTER );
    }

    public boolean upsertEntitySet( FullQualifiedName entityTypeFqn, String entitySetName ) {
        return ps.checkUserHasPermissionsOnEntitySet( currentRoles, entityTypeFqn, entitySetName, Permission.ALTER );
    }

    public boolean deletePropertyType( FullQualifiedName propertyTypeFqn ) {
        return true;
    }

    public boolean deleteEntityType( FullQualifiedName entityTypeFqn ) {
        return ps.checkUserHasPermissionsOnEntityType( currentRoles, entityTypeFqn, Permission.ALTER );
    }

    public boolean deleteEntitySet( FullQualifiedName entityTypeFqn, String entitySetName ) {
        return ps.checkUserHasPermissionsOnEntitySet( currentRoles, entityTypeFqn, entitySetName, Permission.ALTER ) ||
                ps.checkUserHasPermissionsOnEntityType( currentRoles, entityTypeFqn, Permission.ALTER );
    }

    public boolean getEntitySet( FullQualifiedName entityTypeFqn, String entitySetName ) {
        return ps.checkUserHasPermissionsOnEntitySet( currentRoles, entityTypeFqn, entitySetName, Permission.DISCOVER );
    }

    public boolean alterEntityType( FullQualifiedName entityTypeFqn ) {
        return ps.checkUserHasPermissionsOnEntityType( currentRoles, entityTypeFqn, Permission.ALTER );
    }

    public boolean alterEntitySet( FullQualifiedName entityTypeFqn, String entitySetName ) {
        return ps.checkUserHasPermissionsOnEntitySet( currentRoles, entityTypeFqn, entitySetName, Permission.ALTER );
    }

    public boolean assignEntityToEntitySet( FullQualifiedName entityTypeFqn, String entitySetName ) {
        return ps.checkUserHasPermissionsOnEntitySet( currentRoles, entityTypeFqn, entitySetName, Permission.WRITE );
    }

    public boolean readPropertyTypeInEntityType( FullQualifiedName entityTypeFqn, FullQualifiedName propertyTypeFqn ) {
        return ps.checkUserHasPermissionsOnPropertyTypeInEntityType( currentRoles,
                entityTypeFqn,
                propertyTypeFqn,
                Permission.READ );
    }

    public boolean readPropertyTypeInEntitySet(
            FullQualifiedName entityTypeFqn,
            String entitySetName,
            FullQualifiedName propertyTypeFqn ) {
        return ps.checkUserHasPermissionsOnPropertyTypeInEntitySet( currentRoles,
                entityTypeFqn,
                entitySetName,
                propertyTypeFqn,
                Permission.READ );
    }

    public boolean writePropertyTypeInEntityType( FullQualifiedName entityTypeFqn, FullQualifiedName propertyTypeFqn ) {
        return ps.checkUserHasPermissionsOnPropertyTypeInEntityType( currentRoles,
                entityTypeFqn,
                propertyTypeFqn,
                Permission.WRITE );
    }

    public boolean writePropertyTypeInEntitySet(
            FullQualifiedName entityTypeFqn,
            String entitySetName,
            FullQualifiedName propertyTypeFqn ) {
        return ps.checkUserHasPermissionsOnPropertyTypeInEntitySet( currentRoles,
                entityTypeFqn,
                entitySetName,
                propertyTypeFqn,
                Permission.WRITE );
    }

    /**
     * Data Service actions
     */

    public boolean readAllEntitiesOfType( FullQualifiedName entityTypeFqn ) {
        return ps.checkUserHasPermissionsOnEntityType( currentRoles, entityTypeFqn, Permission.READ );
    }

    public boolean getAllEntitiesOfEntitySet( FullQualifiedName entityTypeFqn, String entitySetName ) {
        return ps.checkUserHasPermissionsOnEntitySet( currentRoles, entityTypeFqn, entitySetName, Permission.READ );
    }

    public boolean createEntityOfEntityType( FullQualifiedName entityTypeFqn ) {
        return ps.checkUserHasPermissionsOnEntityType( currentRoles, entityTypeFqn, Permission.WRITE );
    }

    public boolean createEntityOfEntitySet( FullQualifiedName entityTypeFqn, String entitySetName ) {
        return ps.checkUserHasPermissionsOnEntitySet( currentRoles, entityTypeFqn, entitySetName, Permission.WRITE );
    }
}
