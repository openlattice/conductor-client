package com.kryptnostic.conductor.rpc.serializers;

import java.io.IOException;
import java.util.Set;

import org.springframework.stereotype.Component;

import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.dataloom.organizations.processors.EmailDomainsRemover;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.hazelcast.serializers.SetStreamSerializers;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;

@Component
public class EmailDomainsRemoverStreamSerializer implements SelfRegisteringStreamSerializer<EmailDomainsRemover> {

    @Override
    public void write( ObjectDataOutput out, EmailDomainsRemover object ) throws IOException {
        SetStreamSerializers.serialize( out, object.getBackingCollection(), ( String emailDomain ) -> {
            out.writeUTF( emailDomain );
        } );
    }

    @Override
    public EmailDomainsRemover read( ObjectDataInput in ) throws IOException {
        Set<String> emailDomains = SetStreamSerializers.deserialize( in, ( ObjectDataInput dataInput ) -> {
            return dataInput.readUTF();
        } );
        return new EmailDomainsRemover( emailDomains );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.EMAIL_DOMAINS_REMOVER.ordinal();
    }

    @Override
    public void destroy() {}

    @Override
    public Class<EmailDomainsRemover> getClazz() {
        return EmailDomainsRemover.class;
    }

}
