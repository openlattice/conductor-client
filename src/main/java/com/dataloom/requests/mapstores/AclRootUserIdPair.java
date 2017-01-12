package com.dataloom.requests.mapstores;

import java.io.Serializable;
import java.util.List;

import com.dataloom.authorization.AclKeyPathFragment;

public class AclRootUserIdPair implements Serializable {
    private static final long        serialVersionUID = -1687486728573254618L;
    private List<AclKeyPathFragment> aclRoot;
    private String                     userId;

    public AclRootUserIdPair( List<AclKeyPathFragment> aclRoot, String userId ) {
        this.aclRoot = aclRoot;
        this.userId = userId;
    }

    public List<AclKeyPathFragment> getAclRoot() {
        return aclRoot;
    }

    public String getUserId() {
        return userId;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ( ( aclRoot == null ) ? 0 : aclRoot.hashCode() );
        result = prime * result + ( ( userId == null ) ? 0 : userId.hashCode() );
        return result;
    }

    @Override
    public boolean equals( Object obj ) {
        if ( this == obj ) return true;
        if ( obj == null ) return false;
        if ( getClass() != obj.getClass() ) return false;
        AclRootUserIdPair other = (AclRootUserIdPair) obj;
        if ( aclRoot == null ) {
            if ( other.aclRoot != null ) return false;
        } else if ( !aclRoot.equals( other.aclRoot ) ) return false;
        if ( userId == null ) {
            if ( other.userId != null ) return false;
        } else if ( !userId.equals( other.userId ) ) return false;
        return true;
    }

    @Override
    public String toString() {
        return "AclRootUserIdPair [aclRoot=" + aclRoot + ", userId=" + userId + "]";
    }

}
