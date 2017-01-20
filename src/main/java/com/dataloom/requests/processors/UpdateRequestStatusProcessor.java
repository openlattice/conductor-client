package com.dataloom.requests.processors;

import java.util.Map.Entry;

import com.dataloom.requests.PermissionsRequestDetails;
import com.dataloom.requests.RequestStatus;
import com.dataloom.requests.mapstores.AclRootPrincipalPair;
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor;

public class UpdateRequestStatusProcessor
        extends AbstractRhizomeEntryProcessor<AclRootPrincipalPair, PermissionsRequestDetails, Void> {

    private static final long serialVersionUID = 2664646492959861489L;
    private RequestStatus     status;

    public UpdateRequestStatusProcessor( RequestStatus status ) {
        this.status = status;
    }

    @Override
    public Void process( Entry<AclRootPrincipalPair, PermissionsRequestDetails> entry ) {
        /**
        PermissionsRequestDetails details = entry.getValue();
        Preconditions.checkNotNull( details, "Permissions Request does not exist." );
        details.setStatus( status );
        */
        return null;
    }

}
