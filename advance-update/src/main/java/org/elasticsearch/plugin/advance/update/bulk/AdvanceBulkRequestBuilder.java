package org.elasticsearch.plugin.advance.update.bulk;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.WriteRequestBuilder;
import org.elasticsearch.action.support.replication.ReplicationRequest;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.plugin.advance.update.action.UpdateRequest;

/**
 * A bulk request holds an ordered {@link IndexRequest}s and {@link DeleteRequest}s and allows to executes
 * it in a single batch.
 */
public class AdvanceBulkRequestBuilder extends ActionRequestBuilder<AdvanceBulkRequest, AdvanceBulkResponse, AdvanceBulkRequestBuilder>
        implements WriteRequestBuilder<AdvanceBulkRequestBuilder> {

    public AdvanceBulkRequestBuilder(ElasticsearchClient client, AdvanceBulkAction action) {
        super(client, action, new AdvanceBulkRequest());
    }

    /**
     * Adds an {@link IndexRequest} to the list of actions to execute. Follows the same behavior of {@link IndexRequest}
     * (for example, if no id is provided, one will be generated, or usage of the create flag).
     */
    public AdvanceBulkRequestBuilder add(IndexRequest request) {
        super.request.add(request);
        return this;
    }

    /**
     * Adds an {@link IndexRequest} to the list of actions to execute. Follows the same behavior of {@link IndexRequest}
     * (for example, if no id is provided, one will be generated, or usage of the create flag).
     */
    public AdvanceBulkRequestBuilder add(IndexRequestBuilder request) {
        super.request.add(request.request());
        return this;
    }

    /**
     * Adds an {@link DeleteRequest} to the list of actions to execute.
     */
    public AdvanceBulkRequestBuilder add(DeleteRequest request) {
        super.request.add(request);
        return this;
    }

    /**
     * Adds an {@link DeleteRequest} to the list of actions to execute.
     */
    public AdvanceBulkRequestBuilder add(DeleteRequestBuilder request) {
        super.request.add(request.request());
        return this;
    }

    public AdvanceBulkRequestBuilder add(UpdateRequest request) {
        super.request.add(request);
        return this;
    }

    public AdvanceBulkRequestBuilder add(UpdateRequestBuilder request) {
        super.request.add(request.request());
        return this;
    }

    /**
     * Adds a framed data in binary format
     * @deprecated use {@link #add(byte[], int, int, XContentType)}
     */
    @Deprecated
    public AdvanceBulkRequestBuilder add(byte[] data, int from, int length) throws Exception {
        request.add(data, from, length, null, null);
        return this;
    }

    /**
     * Adds a framed data in binary format
     */
    public AdvanceBulkRequestBuilder add(byte[] data, int from, int length, XContentType xContentType) throws Exception {
        request.add(data, from, length, null, null, xContentType);
        return this;
    }

    /**
     * Adds a framed data in binary format
     * @deprecated use {@link #add(byte[], int, int, String, String, XContentType)}
     */
    @Deprecated
    public AdvanceBulkRequestBuilder add(byte[] data, int from, int length, @Nullable String defaultIndex, @Nullable String defaultType) throws Exception {
        request.add(data, from, length, defaultIndex, defaultType);
        return this;
    }

    /**
     * Adds a framed data in binary format
     */
    public AdvanceBulkRequestBuilder add(byte[] data, int from, int length, @Nullable String defaultIndex, @Nullable String defaultType,
                                         XContentType xContentType) throws Exception {
        request.add(data, from, length, defaultIndex, defaultType, xContentType);
        return this;
    }

    /**
     * Sets the number of shard copies that must be active before proceeding with the write.
     * See {@link ReplicationRequest#waitForActiveShards(ActiveShardCount)} for details.
     */
    public AdvanceBulkRequestBuilder setWaitForActiveShards(ActiveShardCount waitForActiveShards) {
        request.waitForActiveShards(waitForActiveShards);
        return this;
    }

    /**
     * A shortcut for {@link #setWaitForActiveShards(ActiveShardCount)} where the numerical
     * shard count is passed in, instead of having to first call {@link ActiveShardCount#from(int)}
     * to get the ActiveShardCount.
     */
    public AdvanceBulkRequestBuilder setWaitForActiveShards(final int waitForActiveShards) {
        return setWaitForActiveShards(ActiveShardCount.from(waitForActiveShards));
    }

    /**
     * A timeout to wait if the index operation can't be performed immediately. Defaults to <tt>1m</tt>.
     */
    public final AdvanceBulkRequestBuilder setTimeout(TimeValue timeout) {
        request.timeout(timeout);
        return this;
    }

    /**
     * A timeout to wait if the index operation can't be performed immediately. Defaults to <tt>1m</tt>.
     */
    public final AdvanceBulkRequestBuilder setTimeout(String timeout) {
        request.timeout(timeout);
        return this;
    }

    /**
     * The number of actions currently in the bulk.
     */
    public int numberOfActions() {
        return request.numberOfActions();
    }
}
