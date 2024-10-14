package com.fdu.msacs.dfs.metanode.mdb;

import java.util.List;
import org.springframework.data.mongodb.repository.MongoRepository;

public interface FileNodeMappingRepo extends MongoRepository<FileNodeMapping, String> {
    FileNodeMapping findByFilename(String filename);
}
