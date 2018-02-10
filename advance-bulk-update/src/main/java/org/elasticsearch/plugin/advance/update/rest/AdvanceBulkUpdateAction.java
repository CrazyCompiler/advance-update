package org.elasticsearch.plugin.advance.update.rest;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugin.advance.update.bulk.AdvanceBulkAction;
import org.elasticsearch.plugin.advance.update.bulk.AdvanceBulkRequest;
import org.elasticsearch.plugin.advance.update.bulk.AdvanceBulkShardRequest;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestStatusToXContentListener;
import org.elasticsearch.rest.action.document.RestBulkAction;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

public class AdvanceBulkUpdateAction extends BaseRestHandler {

    private final boolean allowExplicitIndex;

    private static final DeprecationLogger DEPRECATION_LOGGER =
        new DeprecationLogger(Loggers.getLogger(RestBulkAction.class));

    @Inject
    public AdvanceBulkUpdateAction(final Settings settings, final RestController controller) {
        super(settings);

        String advanceBulkPath = "/_advancebulk";

        controller.registerHandler(POST, advanceBulkPath, this);
        controller.registerHandler(POST, "/{index}"+advanceBulkPath, this);
        controller.registerHandler(POST, "/{index}/{type}"+advanceBulkPath, this);

        controller.registerHandler(PUT, advanceBulkPath, this);
        controller.registerHandler(PUT, "/{index}"+advanceBulkPath, this);
        controller.registerHandler(PUT, "/{index}/{type}"+advanceBulkPath, this);

        this.allowExplicitIndex = MULTI_ALLOW_EXPLICIT_INDEX.get(settings);
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        AdvanceBulkRequest bulkRequest = new AdvanceBulkRequest();
        String defaultIndex = request.param("index");
        String defaultType = request.param("type");
        String defaultRouting = request.param("routing");
        FetchSourceContext defaultFetchSourceContext = FetchSourceContext.parseFromRestRequest(request);
        String fieldsParam = request.param("fields");
        if (fieldsParam != null) {
            DEPRECATION_LOGGER.deprecated("Deprecated field [fields] used, expected [_source] instead");
        }
        String[] defaultFields = fieldsParam != null ? Strings.commaDelimitedListToStringArray(fieldsParam) : null;
        String defaultPipeline = request.param("pipeline");
        String waitForActiveShards = request.param("wait_for_active_shards");
        if (waitForActiveShards != null) {
            bulkRequest.waitForActiveShards(ActiveShardCount.parseString(waitForActiveShards));
        }
        bulkRequest.timeout(request.paramAsTime("timeout", AdvanceBulkShardRequest.DEFAULT_TIMEOUT));
        bulkRequest.setRefreshPolicy(request.param("refresh"));
        bulkRequest.add(request.requiredContent(), defaultIndex, defaultType, defaultRouting, defaultFields,
            defaultFetchSourceContext, defaultPipeline, null, allowExplicitIndex, request.getXContentType());

        return channel -> {
            final ActionListener listener = new RestStatusToXContentListener<>(channel);
            Action instance = AdvanceBulkAction.INSTANCE;
            client.execute(instance, bulkRequest, listener);
        };
    }

    @Override
    public boolean supportsContentStream() {
        return true;
    }
}
