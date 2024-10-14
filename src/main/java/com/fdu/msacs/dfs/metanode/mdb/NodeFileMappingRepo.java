package com.fdu.msacs.dfs.metanode.mdb;

import org.springframework.data.mongodb.repository.MongoRepository;

public interface NodeFileMappingRepo extends MongoRepository<NodeFileMapping, String> {
}
