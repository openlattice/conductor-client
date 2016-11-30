package com.dataloom.authorization;

import java.util.HashSet;

import com.dataloom.authorization.requests.Permission;

public class AccessControlEntry extends HashSet<Permission>{
    
}
    /*implements Entry<String, Set<Permission>> {
}
    String          userId;
    Set<Permission> permissions;

    @Override
    public String getKey() {
        return userId;
    }

    @Override
    public Set<Permission> getValue() {
        return permissions;
    }

    @Override
    public Set<Permission> setValue( Set<Permission> value ) {
        final Set<Permission> oldPermissions = permissions;
        this.permissions = Preconditions.checkNotNull( value );
        return oldPermissions;
    }
}
*/