package com.dataloom.authorization;

public class LazyAcl {
    private final AclKey            aclKey;
    private final Iterable<LazyAce> aces;

    public LazyAcl( AclKey aclKey, Iterable<LazyAce> aces ) {
        this.aclKey = aclKey;
        this.aces = aces;
    }

    public AclKey getAclKey() {
        return aclKey;
    }

    public Iterable<LazyAce> getAces() {
        return aces;
    }

}
