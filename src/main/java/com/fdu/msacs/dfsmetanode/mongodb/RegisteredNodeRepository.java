package com.fdu.msacs.dfsmetanode.mongodb;

import org.springframework.data.mongodb.repository.MongoRepository;

import java.util.List;

public interface RegisteredNodeRepository extends MongoRepository<RegisteredNode, String> {
    RegisteredNode findByNodeUrl(String nodeUrl);
    List<RegisteredNode> findAll();
}
