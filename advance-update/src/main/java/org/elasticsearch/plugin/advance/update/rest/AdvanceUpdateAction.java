package org.elasticsearch.plugin.advance.update.rest;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.plugin.advance.update.action.UpdateAction;
import org.elasticsearch.plugin.advance.update.action.UpdateRequest;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestActions;
import org.elasticsearch.rest.action.RestStatusToXContentListener;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class AdvanceUpdateAction extends BaseRestHandler {
    @Inject
    public AdvanceUpdateAction(final Settings settings, final RestController controller) {
        super(settings);
        controller.registerHandler(POST, "/{index}/{type}/{id}/_advanceupdate", this);
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        UpdateRequest updateRequest = new UpdateRequest(request.param("index"), request.param("type"), request.param("id"));
        updateRequest.routing(request.param("routing"));
        updateRequest.parent(request.param("parent"));
        updateRequest.timeout(request.paramAsTime("timeout", updateRequest.timeout()));
        updateRequest.setRefreshPolicy(request.param("refresh"));
        String waitForActiveShards = request.param("wait_for_active_shards");
        if (waitForActiveShards != null) {
            updateRequest.waitForActiveShards(ActiveShardCount.parseString(waitForActiveShards));
        }
        updateRequest.docAsUpsert(request.paramAsBoolean("doc_as_upsert", updateRequest.docAsUpsert()));
        FetchSourceContext fetchSourceContext = FetchSourceContext.parseFromRestRequest(request);
        String sField = request.param("fields");
        if (sField != null && fetchSourceContext != null) {
            throw new IllegalArgumentException("[fields] and [_source] cannot be used in the same request");
        }
        if (sField != null) {
            String[] sFields = Strings.splitStringByCommaToArray(sField);
            updateRequest.fields(sFields);
        } else if (fetchSourceContext != null) {
            updateRequest.fetchSource(fetchSourceContext);
        }

        updateRequest.retryOnConflict(request.paramAsInt("retry_on_conflict", updateRequest.retryOnConflict()));
        updateRequest.version(RestActions.parseVersion(request));
        updateRequest.versionType(VersionType.fromString(request.param("version_type"), updateRequest.versionType()));

        if (request.hasParam("timestamp")) {
            deprecationLogger.deprecated("The [timestamp] parameter of index requests is deprecated");
        }
        if (request.hasParam("ttl")) {
            deprecationLogger.deprecated("The [ttl] parameter of index requests is deprecated");
        }

        request.applyContentParser(parser -> {
            updateRequest.fromXContent(parser);
            IndexRequest upsertRequest = updateRequest.upsertRequest();
            if (upsertRequest != null) {
                upsertRequest.routing(request.param("routing"));
                upsertRequest.parent(request.param("parent")); // order is important, set it after routing, so it will set the routing
                upsertRequest.timestamp(request.param("timestamp"));
                if (request.hasParam("ttl")) {
                    upsertRequest.ttl(request.param("ttl"));
                }
                upsertRequest.version(RestActions.parseVersion(request));
                upsertRequest.versionType(VersionType.fromString(request.param("version_type"), upsertRequest.versionType()));
            }
            IndexRequest doc = updateRequest.doc();
            if (doc != null) {
                doc.routing(request.param("routing"));
                doc.parent(request.param("parent")); // order is important, set it after routing, so it will set the routing
                doc.timestamp(request.param("timestamp"));
                if (request.hasParam("ttl")) {
                    doc.ttl(request.param("ttl"));
                }
                doc.version(RestActions.parseVersion(request));
                doc.versionType(VersionType.fromString(request.param("version_type"), doc.versionType()));
            }
        });

        return channel -> {
            RestStatusToXContentListener<UpdateResponse> listener = new RestStatusToXContentListener<>(channel, r -> r.getLocation(updateRequest.routing()));
            client.execute(UpdateAction.INSTANCE, updateRequest, listener);
        };
    }
}
