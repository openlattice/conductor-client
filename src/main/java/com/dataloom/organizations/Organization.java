package com.dataloom.organizations;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Set;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;

import com.dataloom.authorization.SecurableObjectType;
import com.dataloom.authorization.requests.Principal;
import com.dataloom.data.SerializationConstants;
import com.dataloom.edm.internal.AbstractSecurableObject;
import com.dataloom.organizations.HazelcastOrganizationService.Visibility;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;

public class Organization extends AbstractSecurableObject {

    private static final long    serialVersionUID = -669072251620432197L;
    private final String         title;
    private final String         description;
    private final Visibility     visibility;
    private final Set<UUID>      trustedOrganizations;
    private final Set<String>    autoApprovedEmails;
    private final Set<Principal> members;
    private final Set<Principal> roles;

    @JsonCreator
    public Organization(
            Optional<UUID> id,
            @JsonProperty( SerializationConstants.TITLE_FIELD ) String title,
            @JsonProperty( SerializationConstants.DESCRIPTION_FIELD ) Optional<String> description,
            @JsonProperty( SerializationConstants.VISIBILITY_FIELD ) Visibility visibility,
            @JsonProperty( SerializationConstants.TRUSTED_ORGANIZATIONS_FIELD ) Set<UUID> trustedOrganizations,
            @JsonProperty( SerializationConstants.EMAILS_FIELD ) Set<String> autoApprovedEmails,
            @JsonProperty( SerializationConstants.MEMBERS_FIELD ) Set<Principal> members,
            @JsonProperty( SerializationConstants.ROLES ) Set<Principal> roles ) {
        super( id.or( UUID::randomUUID ), id.isPresent() );
        /*
         * There is no logical requirement that the title not be blank, it would just be very confusing to have a bunch
         * of organizations with no title whatsoever. This can be relaxed in the future.
         */
        checkArgument( StringUtils.isNotBlank( title ), "Title cannot be blank." );
        this.title = title;
        this.description = description.or( "" );
        this.visibility = checkNotNull( visibility );
        this.trustedOrganizations = checkNotNull( trustedOrganizations );
        this.autoApprovedEmails = checkNotNull( autoApprovedEmails );
        this.members = checkNotNull( members );
        this.roles = checkNotNull( roles );
    }

    @JsonProperty( SerializationConstants.TITLE_FIELD )
    public String getTitle() {
        return title;
    }

    @JsonProperty( SerializationConstants.DESCRIPTION_FIELD )
    public String getDescription() {
        return description;
    }

    @JsonProperty( SerializationConstants.VISIBILITY_FIELD )
    public Visibility getVisibility() {
        return visibility;
    }

    @JsonProperty( SerializationConstants.TRUSTED_ORGANIZATIONS_FIELD )
    public Set<UUID> getTrustedOrganizations() {
        return trustedOrganizations;
    }
    
    @JsonProperty( SerializationConstants.EMAILS_FIELD )
    public Set<String> getAutoApprovedEmails() {
        return autoApprovedEmails;
    }

    @JsonProperty( SerializationConstants.MEMBERS_FIELD )
    public Set<Principal> getMembers() {
        return members;
    }

    @JsonProperty( SerializationConstants.ROLES )
    public Set<Principal> getRoles() {
        return roles;
    }

    @Override
    @JsonIgnore
    public SecurableObjectType getCategory() {
        return SecurableObjectType.Organization;
    }

}
