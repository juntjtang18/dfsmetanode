package com.fdu.msacs.dfsmetanode.mongodb;

import org.springframework.data.mongodb.repository.MongoRepository;

import java.util.List;

public interface NodeFileMappingRepository extends MongoRepository<NodeFiles, String> {
    NodeFiles findByNodeUrl(String nodeUrl);
}
