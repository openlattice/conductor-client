package com.dataloom.authorization;

import java.util.List;
import java.util.Map;

public class AclKeyTree {
    Map<List<AclKeyPathFragment>, AclKeyTree> children;
}
