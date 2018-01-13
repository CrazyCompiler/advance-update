package org.elasticsearch.plugin.advance.update.bulk;

import org.elasticsearch.action.Action;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.transport.TransportRequestOptions;

public class AdvanceBulkAction extends Action<AdvanceBulkRequest, AdvanceBulkResponse, AdvanceBulkRequestBuilder> {

    public static final AdvanceBulkAction INSTANCE = new AdvanceBulkAction();
    public static final String NAME = "indices:data/write/advancebulk";

    private AdvanceBulkAction() {
        super(NAME);
    }

    @Override
    public AdvanceBulkResponse newResponse() {
        return new AdvanceBulkResponse();
    }

    @Override
    public AdvanceBulkRequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new AdvanceBulkRequestBuilder(client, this);
    }

    @Override
    public TransportRequestOptions transportOptions(Settings settings) {
        return TransportRequestOptions.builder()
            .withType(TransportRequestOptions.Type.BULK)
            .withCompress(settings.getAsBoolean("action.bulk.compress", true)
            ).build();
    }
}
