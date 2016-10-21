package com.kryptnostic.datastore.services.requests;

import java.util.Set;
import java.util.UUID;

import com.kryptnostic.datastore.Permission;

public class AclRequest {
    private final UUID userId;
    private final Action action;
    private final Set<Permission> permissions;
    
    public AclRequest(
            UUID userId,
            Action action,
            Set<Permission> permissions){
        this.userId = userId;
        this.action = action;
        this.permissions = permissions;        
    }
}
