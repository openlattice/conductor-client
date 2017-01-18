package com.dataloom.organizations.processors;

import java.util.UUID;

import com.dataloom.organizations.EmailDomains;
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRemover;

public class EmailDomainsRemover extends AbstractRemover<UUID, EmailDomains, String> {
    private static final long serialVersionUID = -4808156947180508536L;

    public EmailDomainsRemover( Iterable<String> objects ) {
        super( objects );
    }
}
