package org.elasticsearch.plugin.advance.update.bulk;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.support.WriteResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;

/**
 *
 */
public class AdvanceBulkShardResponse extends ReplicationResponse implements WriteResponse {

    private ShardId shardId;
    private AdvanceBulkItemResponse[] responses;

    AdvanceBulkShardResponse() {
    }

    AdvanceBulkShardResponse(ShardId shardId, AdvanceBulkItemResponse[] responses) {
        this.shardId = shardId;
        this.responses = responses;
    }

    public ShardId getShardId() {
        return shardId;
    }

    public AdvanceBulkItemResponse[] getResponses() {
        return responses;
    }

    @Override
    public void setForcedRefresh(boolean forcedRefresh) {
        /*
         * Each DocWriteResponse already has a location for whether or not it forced a refresh so we just set that information on the
         * response.
         */
        for (AdvanceBulkItemResponse response : responses) {
            DocWriteResponse r = response.getResponse();
            if (r != null) {
                r.setForcedRefresh(forcedRefresh);
            }
        }
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        shardId = ShardId.readShardId(in);
        responses = new AdvanceBulkItemResponse[in.readVInt()];
        for (int i = 0; i < responses.length; i++) {
            responses[i] = AdvanceBulkItemResponse.readBulkItem(in);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        shardId.writeTo(out);
        out.writeVInt(responses.length);
        for (AdvanceBulkItemResponse response : responses) {
            response.writeTo(out);
        }
    }
}
