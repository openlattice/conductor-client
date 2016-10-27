package com.kryptnostic.datastore.services;

import java.util.Set;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.kryptnostic.datastore.Permission;

public class ActionAuthorizationService {
    
    private final PermissionsService ps;
    public ActionAuthorizationService ( PermissionsService ps ) {
        this.ps = ps;
    };
    
    /**
     * Entity Data Model actions
     */
    
    public boolean upsertPropertyType( Set<String> roles, FullQualifiedName propertyTypeFqn){
        return ps.checkUserHasPermissionsOnPropertyType( roles, propertyTypeFqn, Permission.ALTER );
    }
    
    public boolean upsertEntityType( Set<String> roles, FullQualifiedName entityTypeFqn){
        return ps.checkUserHasPermissionsOnEntityType( roles, entityTypeFqn, Permission.ALTER );
    }
    
    public boolean upsertEntitySet( Set<String> roles, FullQualifiedName entityTypeFqn, String entitySetName){
        return ps.checkUserHasPermissionsOnEntitySet( roles, entityTypeFqn, entitySetName, Permission.ALTER );
    }
    
    public boolean deletePropertyType( Set<String> roles, FullQualifiedName propertyTypeFqn ){
        return ps.checkUserHasPermissionsOnPropertyType( roles, propertyTypeFqn, Permission.OWNER );
    }
    
    public boolean deleteEntityType( Set<String> roles, FullQualifiedName entityTypeFqn ){
        return ps.checkUserHasPermissionsOnEntityType( roles, entityTypeFqn, Permission.OWNER );
    }
    
    public boolean deleteEntitySet( Set<String> roles, FullQualifiedName entityTypeFqn, String entitySetName ){
        return ps.checkUserHasPermissionsOnEntitySet( roles, entityTypeFqn, entitySetName, Permission.OWNER ) || 
               ps.checkUserHasPermissionsOnEntityType( roles, entityTypeFqn, Permission.OWNER );
    }
    
    public boolean getPropertyType( Set<String> roles, FullQualifiedName propertyTypeFqn ){
        return ps.checkUserHasPermissionsOnPropertyType( roles, propertyTypeFqn, Permission.DISCOVER );
    }
    
    public boolean getEntityType( Set<String> roles, FullQualifiedName entityTypeFqn ){
        return ps.checkUserHasPermissionsOnEntityType( roles, entityTypeFqn, Permission.DISCOVER );
    }
    
    public boolean getEntitySet( Set<String> roles, FullQualifiedName entityTypeFqn, String entitySetName ){
        return ps.checkUserHasPermissionsOnEntitySet( roles, entityTypeFqn, entitySetName, Permission.DISCOVER );
    }

    public boolean alterEntityType( Set<String> roles, FullQualifiedName entityTypeFqn ){
        return ps.checkUserHasPermissionsOnEntityType( roles, entityTypeFqn, Permission.ALTER );
    }
    
    public boolean alterEntitySet( Set<String> roles, FullQualifiedName entityTypeFqn, String entitySetName ){
        return ps.checkUserHasPermissionsOnEntitySet( roles, entityTypeFqn, entitySetName, Permission.ALTER );
    }
    
    public boolean assignEntityToEntitySet( Set<String> roles, FullQualifiedName entityTypeFqn, String entitySetName ){
        return ps.checkUserHasPermissionsOnEntitySet( roles, entityTypeFqn, entitySetName, Permission.WRITE );
    }

    public boolean getPropertyTypeInEntityType( Set<String> roles, FullQualifiedName entityTypeFqn, FullQualifiedName propertyTypeFqn ){
        return ps.checkUserHasPermissionsOnPropertyTypeInEntityType( roles, entityTypeFqn, propertyTypeFqn, Permission.DISCOVER );
    }

    public boolean getPropertyTypeInEntitySet( Set<String> roles, FullQualifiedName entityTypeFqn, String entitySetName, FullQualifiedName propertyTypeFqn ){
        return ps.checkUserHasPermissionsOnPropertyTypeInEntitySet( roles, entityTypeFqn, entitySetName, propertyTypeFqn, Permission.DISCOVER );
    }

    public boolean readPropertyTypeInEntityType( Set<String> roles, FullQualifiedName entityTypeFqn, FullQualifiedName propertyTypeFqn ){
        return ps.checkUserHasPermissionsOnPropertyTypeInEntityType( roles, entityTypeFqn, propertyTypeFqn, Permission.READ );
    }

    public boolean readPropertyTypeInEntitySet( Set<String> roles, FullQualifiedName entityTypeFqn, String entitySetName, FullQualifiedName propertyTypeFqn ){
        return ps.checkUserHasPermissionsOnPropertyTypeInEntitySet( roles, entityTypeFqn, entitySetName, propertyTypeFqn, Permission.READ );
    }

    public boolean writePropertyTypeInEntityType( Set<String> roles, FullQualifiedName entityTypeFqn, FullQualifiedName propertyTypeFqn ){
        return ps.checkUserHasPermissionsOnPropertyTypeInEntityType( roles, entityTypeFqn, propertyTypeFqn, Permission.WRITE );
    }
    
    public boolean writePropertyTypeInEntitySet( Set<String> roles, FullQualifiedName entityTypeFqn, String entitySetName, FullQualifiedName propertyTypeFqn ){
        return ps.checkUserHasPermissionsOnPropertyTypeInEntitySet( roles, entityTypeFqn, entitySetName, propertyTypeFqn, Permission.WRITE );
    }

    /**
     * Data Service actions
     */
    
    public boolean readAllEntitiesOfType( Set<String> roles, FullQualifiedName entityTypeFqn){
        return ps.checkUserHasPermissionsOnEntityType( roles, entityTypeFqn, Permission.READ );
    }
    
    public boolean getAllEntitiesOfEntitySet( Set<String> roles, FullQualifiedName entityTypeFqn, String entitySetName ){
        return ps.checkUserHasPermissionsOnEntitySet( roles, entityTypeFqn, entitySetName, Permission.READ );
    }
    
    public boolean createEntityOfEntityType( Set<String> roles, FullQualifiedName entityTypeFqn ){
        return ps.checkUserHasPermissionsOnEntityType( roles, entityTypeFqn, Permission.WRITE );
    }

    public boolean createEntityOfEntitySet( Set<String> roles, FullQualifiedName entityTypeFqn, String entitySetName ){
        return ps.checkUserHasPermissionsOnEntitySet( roles, entityTypeFqn, entitySetName, Permission.WRITE );
    }
}
