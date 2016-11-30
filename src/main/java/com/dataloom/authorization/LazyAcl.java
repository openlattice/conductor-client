package com.dataloom.authorization;

public class LazyAcl {
    private final AclKey            aclKey;
    private final Iterable<Ace> aces;

    public LazyAcl( AclKey aclKey, Iterable<Ace> aces ) {
        this.aclKey = aclKey;
        this.aces = aces;
    }

    public AclKey getAclKey() {
        return aclKey;
    }

    public Iterable<Ace> getAces() {
        return aces;
    }

}
