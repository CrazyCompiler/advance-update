package org.elasticsearch.plugin.advance.update.bulk;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.StatusToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentParserUtils.*;
import static org.elasticsearch.plugin.advance.update.bulk.AdvanceBulkItemResponse.readBulkItem;

public class AdvanceBulkResponse extends ActionResponse implements Iterable<AdvanceBulkItemResponse>, StatusToXContentObject {

    private static final String ITEMS = "items";
    private static final String ERRORS = "errors";
    private static final String TOOK = "took";
    private static final String INGEST_TOOK = "ingest_took";

    public static final long NO_INGEST_TOOK = -1L;

    private AdvanceBulkItemResponse[] responses;
    private long tookInMillis;
    private long ingestTookInMillis;

    AdvanceBulkResponse() {
    }

    public AdvanceBulkResponse(AdvanceBulkItemResponse[] responses, long tookInMillis) {
        this(responses, tookInMillis, NO_INGEST_TOOK);
    }

    public AdvanceBulkResponse(AdvanceBulkItemResponse[] responses, long tookInMillis, long ingestTookInMillis) {
        this.responses = responses;
        this.tookInMillis = tookInMillis;
        this.ingestTookInMillis = ingestTookInMillis;
    }

    /**
     * How long the bulk execution took. Excluding ingest preprocessing.
     */
    public TimeValue getTook() {
        return new TimeValue(tookInMillis);
    }

    /**
     * How long the bulk execution took in milliseconds. Excluding ingest preprocessing.
     */
    public long getTookInMillis() {
        return tookInMillis;
    }

    /**
     * If ingest is enabled returns the bulk ingest preprocessing time, otherwise 0 is returned.
     */
    public TimeValue getIngestTook() {
        return new TimeValue(ingestTookInMillis);
    }

    /**
     * If ingest is enabled returns the bulk ingest preprocessing time. in milliseconds, otherwise -1 is returned.
     */
    public long getIngestTookInMillis() {
        return ingestTookInMillis;
    }

    /**
     * Has anything failed with the execution.
     */
    public boolean hasFailures() {
        for (AdvanceBulkItemResponse response : responses) {
            if (response.isFailed()) {
                return true;
            }
        }
        return false;
    }

    public String buildFailureMessage() {
        StringBuilder sb = new StringBuilder();
        sb.append("failure in bulk execution:");
        for (int i = 0; i < responses.length; i++) {
            AdvanceBulkItemResponse response = responses[i];
            if (response.isFailed()) {
                sb.append("\n[").append(i)
                        .append("]: index [").append(response.getIndex()).append("], type [").append(response.getType()).append("], id [").append(response.getId())
                        .append("], message [").append(response.getFailureMessage()).append("]");
            }
        }
        return sb.toString();
    }

    /**
     * The items representing each action performed in the bulk operation (in the same order!).
     */
    public AdvanceBulkItemResponse[] getItems() {
        return responses;
    }

    @Override
    public Iterator<AdvanceBulkItemResponse> iterator() {
        return Arrays.stream(responses).iterator();
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        responses = new AdvanceBulkItemResponse[in.readVInt()];
        for (int i = 0; i < responses.length; i++) {
            responses[i] = readBulkItem(in);
        }
        tookInMillis = in.readVLong();
        ingestTookInMillis = in.readZLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(responses.length);
        for (AdvanceBulkItemResponse response : responses) {
            response.writeTo(out);
        }
        out.writeVLong(tookInMillis);
        out.writeZLong(ingestTookInMillis);
    }

    @Override
    public RestStatus status() {
        return RestStatus.OK;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(TOOK, tookInMillis);
        if (ingestTookInMillis != AdvanceBulkResponse.NO_INGEST_TOOK) {
            builder.field(INGEST_TOOK, ingestTookInMillis);
        }
        builder.field(ERRORS, hasFailures());
        builder.startArray(ITEMS);
        for (AdvanceBulkItemResponse item : this) {
            item.toXContent(builder, params);
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    public static AdvanceBulkResponse fromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.nextToken();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser::getTokenLocation);

        long took = -1L;
        long ingestTook = NO_INGEST_TOOK;
        List<AdvanceBulkItemResponse> items = new ArrayList<>();

        String currentFieldName = parser.currentName();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (TOOK.equals(currentFieldName)) {
                    took = parser.longValue();
                } else if (INGEST_TOOK.equals(currentFieldName)) {
                    ingestTook = parser.longValue();
                } else if (ERRORS.equals(currentFieldName) == false) {
                    throwUnknownField(currentFieldName, parser.getTokenLocation());
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (ITEMS.equals(currentFieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        items.add(AdvanceBulkItemResponse.fromXContent(parser, items.size()));
                    }
                } else {
                    throwUnknownField(currentFieldName, parser.getTokenLocation());
                }
            } else {
                throwUnknownToken(token, parser.getTokenLocation());
            }
        }
        return new AdvanceBulkResponse(items.toArray(new AdvanceBulkItemResponse[items.size()]), took, ingestTook);
    }
}
