package com.fdu.msacs.dfs.metanode.mongodb;

import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Repository;
import org.springframework.stereotype.Service;

import java.util.List;

@Repository
public interface RegisteredNodeRepository extends MongoRepository<RegisteredNode, String> {
    RegisteredNode findByNodeUrl(String nodeUrl);
    List<RegisteredNode> findAll(Sort sort);
}
