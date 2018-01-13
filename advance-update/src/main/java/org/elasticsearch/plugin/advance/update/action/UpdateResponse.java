package org.elasticsearch.plugin.advance.update.action;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.index.shard.ShardId;

public class UpdateResponse extends org.elasticsearch.action.update.UpdateResponse {

    public UpdateResponse(ShardId shardId, String type, String id, long version, Result noop) {
        super(shardId,type,id,version,noop);
    }

    public UpdateResponse(ShardInfo shardInfo, ShardId shardId, String type, String id, long version, Result result) {
        super(shardInfo,shardId,type,id,version,result);
    }

    public UpdateResponse() {
        super();
    }
}
