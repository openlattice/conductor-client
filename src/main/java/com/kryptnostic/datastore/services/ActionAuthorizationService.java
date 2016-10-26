package com.kryptnostic.datastore.services;

import java.util.Set;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.kryptnostic.datastore.Permission;

public class ActionAuthorizationService {
    
    private final PermissionsService ps;
    public ActionAuthorizationService ( PermissionsService ps ) {
        this.ps = ps;
    };
    
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
        return ps.checkUserHasPermissionsOnPropertyType( roles, propertyTypeFqn, Permission.ALTER );
    }
    
    public boolean deleteEntityType( Set<String> roles, FullQualifiedName entityTypeFqn ){
        return ps.checkUserHasPermissionsOnEntityType( roles, entityTypeFqn, Permission.ALTER );
    }
    
    public boolean deleteEntitySet( Set<String> roles, FullQualifiedName entityTypeFqn, String entitySetName ){
        return ps.checkUserHasPermissionsOnEntitySet( roles, entityTypeFqn, entitySetName, Permission.ALTER );
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
}
