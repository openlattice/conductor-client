package com.dataloom.authorization.paging;

import com.openlattice.authorization.Principal;
import com.datastax.driver.core.PagingState;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

/**
 * Represents all the information needed to do paging when querying backend for authorized objects of a user with a
 * specified securable object type and specified permission. Principal cannot be null, whereas PagingState could be
 * null. (This corresponds to the first ever query made for this principal)
 * 
 * @author Ho Chung Siu
 *
 */
public class AuthorizedObjectsPagingInfo {
    private static final String PRINCIPAL    = "principal";
    private static final String PAGING_STATE = "pagingState";

    private Principal           principal;
    private PagingState         pagingState;

    public AuthorizedObjectsPagingInfo(
            Principal principal,
            PagingState pagingState ) {
        this.principal = Preconditions.checkNotNull( principal );
        this.pagingState = pagingState;
    }

    @JsonCreator
    public AuthorizedObjectsPagingInfo(
            @JsonProperty( PRINCIPAL ) Principal principal,
            @JsonProperty( PAGING_STATE ) String pagingStateString ) {
        this( principal, ( pagingStateString == null ) ? null : PagingState.fromString( pagingStateString ) );
    }

    @JsonProperty( PRINCIPAL )
    public Principal getPrincipal() {
        return principal;
    }

    @JsonIgnore
    public PagingState getPagingState() {
        return pagingState;
    }

    @JsonProperty( PAGING_STATE )
    public String getPagingStateString() {
        return pagingState == null ? null : pagingState.toString();
    }

    @Override
    public String toString() {
        return "AuthorizedObjectsPagingInfo [principal=" + principal + ", pagingState=" + pagingState + "]";
    }

}
