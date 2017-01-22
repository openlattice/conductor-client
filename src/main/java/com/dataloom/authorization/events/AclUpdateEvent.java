package com.dataloom.authorization.events;

import java.util.List;
import java.util.Set;
import java.util.UUID;

import com.dataloom.authorization.Principal;

public class AclUpdateEvent {
    
    private List<UUID> aclKeys;
    private Set<Principal> principals;
    
    public AclUpdateEvent( List<UUID> aclKeys, Set<Principal> principals ) {
        this.aclKeys = aclKeys;
        this.principals = principals;
    }
    
    public List<UUID> getAclKeys() {
        return aclKeys;
    }
    
    public Set<Principal> getPrincipals() {
        return principals;
    }

}
