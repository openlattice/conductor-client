package com.dataloom.authorization;

import java.util.List;

public class Acl {
    private final List<AclKey>  aclKey;
    private final Iterable<Ace> aces;

    public Acl( List<AclKey> aclKey, Iterable<Ace> aces ) {
        this.aclKey = aclKey;
        this.aces = aces;
    }

    public List<AclKey> getAclKey() {
        return aclKey;
    }

    public Iterable<Ace> getAces() {
        return aces;
    }

}
