package com.kryptnostic.conductor.rpc;

import java.io.Serializable;
import java.util.List;

public interface ConductorSparkApi extends Serializable {
    List<Employee> processEmployees();
}
