package com.dataloom.neuron.pods;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.springframework.context.annotation.Configuration;

import com.dataloom.data.DatasourceManager;
import com.dataloom.neuron.audit.AuditEntitySetUtils;
import com.kryptnostic.datastore.services.EdmManager;

@Configuration
public class AuditEntitySetPod {

    @Inject
    private DatasourceManager dataSourceManager;

    @Inject
    private EdmManager entityDataModelManager;

    @PostConstruct
    public void initializeAuditEntitySet() {
        AuditEntitySetUtils.initialize( dataSourceManager, entityDataModelManager );
    }
}
