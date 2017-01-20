package com.kryptnostic.conductor.rpc.serializers;

import java.io.IOException;

import org.springframework.stereotype.Component;

import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.conductor.rpc.Employee;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;

@Component
public class EmployeeStreamSerializer implements SelfRegisteringStreamSerializer<Employee> {

    @Override
    public void write( ObjectDataOutput out, Employee object ) throws IOException {
        out.writeUTF( object.getName() );
        out.writeUTF( object.getTitle() );
        out.writeUTF( object.getDept() );
        out.writeInt( object.getSalary() );

    }

    @Override
    public Employee read( ObjectDataInput in ) throws IOException {
        return new Employee(
                in.readUTF(),
                in.readUTF(),
                in.readUTF(),
                in.readInt() );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.EMPLOYEE.ordinal();
    }

    @Override
    public void destroy() {}

    @Override
    public Class<Employee> getClazz() {
        return Employee.class;
    }

}
