package org.elasticsearch.plugin.advance.update.bulk;

import org.elasticsearch.Version;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;

public class AdvanceBulkItemRequest implements Streamable {

    private int id;
    private DocWriteRequest request;
    private volatile AdvanceBulkItemResponse primaryResponse;

    AdvanceBulkItemRequest() {

    }

    public AdvanceBulkItemRequest(int id, DocWriteRequest request) {
        assert request instanceof IndicesRequest;
        this.id = id;
        this.request = request;
    }

    public int id() {
        return id;
    }

    public DocWriteRequest request() {
        return request;
    }

    public String index() {
        assert request.indices().length == 1;
        return request.indices()[0];
    }

    AdvanceBulkItemResponse getPrimaryResponse() {
        return primaryResponse;
    }

    void setPrimaryResponse(AdvanceBulkItemResponse primaryResponse) {
        this.primaryResponse = primaryResponse;
    }

    boolean isIgnoreOnReplica() {
        return primaryResponse != null &&
            (primaryResponse.isFailed() || primaryResponse.getResponse().getResult() == DocWriteResponse.Result.NOOP);
    }

    public static AdvanceBulkItemRequest readBulkItem(StreamInput in) throws IOException {
        AdvanceBulkItemRequest item = new AdvanceBulkItemRequest();
        item.readFrom(in);
        return item;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        id = in.readVInt();
        request = DocWriteRequest.readDocumentRequest(in);
        if (in.readBoolean()) {
            primaryResponse = AdvanceBulkItemResponse.readBulkItem(in);
            // This is a bwc layer for 6.0 which no longer mutates the requests with these
            // Since 5.x still requires it we do it here. Note that these are harmless
            // as both operations are idempotent. This is something we rely on and assert on
            // in InternalEngine.planIndexingAsNonPrimary()
            request.version(primaryResponse.getVersion());
            request.versionType(request.versionType().versionTypeForReplicationAndRecovery());
        }
        if (in.getVersion().before(Version.V_5_6_0_UNRELEASED)) {
            boolean ignoreOnReplica = in.readBoolean();
            assert ignoreOnReplica == isIgnoreOnReplica() :
                "ignoreOnReplica mismatch. wire [" + ignoreOnReplica + "], ours [" + isIgnoreOnReplica() + "]";
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(id);
        DocWriteRequest.writeDocumentRequest(out, request);
        System.out.println("The request is : "+request);
        out.writeOptionalStreamable(primaryResponse);
        if (out.getVersion().before(Version.V_5_6_0_UNRELEASED)) {
            out.writeBoolean(isIgnoreOnReplica());
        }
    }
}
