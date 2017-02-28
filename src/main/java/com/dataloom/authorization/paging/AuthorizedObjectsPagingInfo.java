package com.dataloom.authorization.paging;

import com.dataloom.authorization.Principal;
import com.datastax.driver.core.PagingState;
import com.google.common.base.Preconditions;

/**
 * Represents all the information needed to do paging when querying backend for authorized objects of a user with a specified securable object type and specified permission.
 * Principal cannot be null, whereas PagingState could be null. (This corresponds to the first ever query made for this principal)
 * @author Ho Chung Siu
 *
 */
public class AuthorizedObjectsPagingInfo {
    private Principal   principal;
    private PagingState pagingState;

    public AuthorizedObjectsPagingInfo( Principal principal, PagingState pagingState ) {
        this.principal = Preconditions.checkNotNull( principal );
        this.pagingState = pagingState;
    }

    public Principal getPrincipal() {
        return principal;
    }

    public PagingState getPagingState() {
        return pagingState;
    }

    @Override
    public String toString() {
        return "AuthorizedObjectsPagingInfo [principal=" + principal + ", pagingState=" + pagingState + "]";
    }

}
