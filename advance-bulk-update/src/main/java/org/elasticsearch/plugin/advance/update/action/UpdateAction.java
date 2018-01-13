package org.elasticsearch.plugin.advance.update.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.client.ElasticsearchClient;

/**
 */
public class UpdateAction extends Action {

    public static final UpdateAction INSTANCE = new UpdateAction();
    public static final String NAME = "indices:data/write/advanceupdate";

    private UpdateAction() {
        super(NAME);
    }

    @Override
    public UpdateResponse newResponse() {
        return new UpdateResponse();
    }

    @Override
    public UpdateRequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new UpdateRequestBuilder(client, this);
    }
}
