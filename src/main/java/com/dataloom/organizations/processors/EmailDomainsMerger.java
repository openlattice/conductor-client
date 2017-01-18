package com.dataloom.organizations.processors;

import java.util.UUID;

import com.dataloom.organizations.EmailDomains;
import com.kryptnostic.rhizome.hazelcast.processors.AbstractMerger;

import jersey.repackaged.com.google.common.collect.Sets;

public class EmailDomainsMerger extends AbstractMerger<UUID, EmailDomains, String> {
    public EmailDomainsMerger( Iterable<String> objects ) {
        super( objects );
    }

    private static final long serialVersionUID = -6923080316858930293L;

    @Override
    protected EmailDomains newEmptyCollection() {
        return new EmailDomains( Sets.newHashSet() );
    }

}
