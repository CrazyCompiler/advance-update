package org.elasticsearch.plugin.advance.update;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.GenericAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.plugin.advance.update.action.TransportUpdateAction;
import org.elasticsearch.plugin.advance.update.action.UpdateAction;
import org.elasticsearch.plugin.advance.update.rest.AdvanceUpdateAction;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

public class AdvanceUpdate extends Plugin implements ActionPlugin {

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        GenericAction instance = UpdateAction.INSTANCE;
        Class<TransportUpdateAction> transportUpdateActionClass = TransportUpdateAction.class;
        return Collections.singletonList(new ActionHandler<>(instance, transportUpdateActionClass));
    }

    @Override
    public List<RestHandler> getRestHandlers(Settings settings,
                                             RestController restController,
                                             ClusterSettings clusterSettings,
                                             IndexScopedSettings indexScopedSettings,
                                             SettingsFilter settingsFilter,
                                             IndexNameExpressionResolver indexNameExpressionResolver,
                                             Supplier<DiscoveryNodes> nodesInCluster) {
        AdvanceUpdateAction handler = new AdvanceUpdateAction(settings, restController);
        List<RestHandler> listHandlers = new ArrayList<>();
        listHandlers.add(handler);
        return listHandlers;
    }

}
