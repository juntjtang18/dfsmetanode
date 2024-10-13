package com.fdu.msacs.dfs.metanode.mongodb;

import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Repository;
import org.springframework.stereotype.Service;

import java.util.List;

@Repository
public interface NodeFileMappingRepository extends MongoRepository<NodeFiles, String> {
    NodeFiles findByNodeUrl(String nodeUrl);
}
