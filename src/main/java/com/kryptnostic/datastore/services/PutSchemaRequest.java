package com.kryptnostic.datastore.services;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;

import java.util.UUID;

/**
 * Created by yao on 9/26/16.
 */
public class PutSchemaRequest {
    private final String         namespace;
    private final String         name;
    private final Optional<UUID> aclId;

    @JsonCreator
    public PutSchemaRequest(
            @JsonProperty( EdmApi.NAMESPACE ) String namespace,
            @JsonProperty( EdmApi.NAME ) String name,
            @JsonProperty( EdmApi.ACL_ID ) Optional<UUID> aclId ) {
        this.namespace = namespace;
        this.name = name;
        this.aclId = aclId;
    }

    @JsonProperty( EdmApi.NAMESPACE )
    public String getNamespace() {
        return namespace;
    }

    @JsonProperty( EdmApi.NAME )
    public String getName() {
        return name;
    }

    @JsonProperty( EdmApi.ACL_ID )
    public Optional<UUID> getAclId() {
        return aclId;
    }
}
