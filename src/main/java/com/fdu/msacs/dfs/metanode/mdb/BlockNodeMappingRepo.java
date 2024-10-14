package com.fdu.msacs.dfs.metanode.mdb;
import java.util.Optional;

import org.springframework.data.mongodb.repository.MongoRepository;

public interface BlockNodeMappingRepo extends MongoRepository<BlockNode, String> {
    BlockNode findByHash(String hash);
}