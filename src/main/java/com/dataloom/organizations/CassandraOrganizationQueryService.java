package com.dataloom.organizations;

import java.util.UUID;

import com.datastax.driver.core.Session;

public class CassandraOrganizationQueryService {
    private final Session session;
    public CassandraOrganizationQueryService( String keyspace, Session session ) {
        this.session = session;
    }
    
    public Iterable<UUID> getOrganizationIds() {
        return null;
    }
}
