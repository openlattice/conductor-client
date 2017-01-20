package com.dataloom.requests.mapstores;

import java.util.List;
import java.util.UUID;

import com.dataloom.authorization.Principal;

public class AclRootPrincipalPair {
    private List<UUID> aclRoot;
    private Principal  user;

    public AclRootPrincipalPair( List<UUID> aclRoot, Principal user ) {
        this.aclRoot = aclRoot;
        this.user = user;
    }

    public List<UUID> getAclRoot() {
        return aclRoot;
    }

    public Principal getUser() {
        return user;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ( ( aclRoot == null ) ? 0 : aclRoot.hashCode() );
        result = prime * result + ( ( user == null ) ? 0 : user.hashCode() );
        return result;
    }

    @Override
    public boolean equals( Object obj ) {
        if ( this == obj ) return true;
        if ( obj == null ) return false;
        if ( getClass() != obj.getClass() ) return false;
        AclRootPrincipalPair other = (AclRootPrincipalPair) obj;
        if ( aclRoot == null ) {
            if ( other.aclRoot != null ) return false;
        } else if ( !aclRoot.equals( other.aclRoot ) ) return false;
        if ( user == null ) {
            if ( other.user != null ) return false;
        } else if ( !user.equals( other.user ) ) return false;
        return true;
    }

    @Override
    public String toString() {
        return "AclRootPrincipalPair [aclRoot=" + aclRoot + ", principal=" + user + "]";
    }

}
