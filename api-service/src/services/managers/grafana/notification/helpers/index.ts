import _ from "lodash";
import { grafanaHttpClient } from "../../../../../connections/grafanaConnection";
import { getChannelService } from '../channels';
import defaultTemplates from '../templates';

const generateChannelConfig = (payload: Record<string, any>) => {
    const { type } = payload;
    const channel = getChannelService(type);
    return channel.service.generateConfigPayload(payload);
};

const getAlertManagerConfig = async () => {
    return grafanaHttpClient.get("/api/alertmanager/grafana/config/api/v1/alerts")
        .then(response => response.data);
};

const updateAlertManagerConfig = async (payload: Record<string, any>) => {
    return grafanaHttpClient.post("/api/alertmanager/grafana/config/api/v1/alerts", payload);
};

const getReceivers = (alertmanager_config: Record<string, any>) => _.get(alertmanager_config, 'alertmanager_config.receivers') as Array<any>;
const getRoutes = (alertmanager_config: Record<string, any>) => _.get(alertmanager_config, 'alertmanager_config.route.routes') as Array<any>;
const getTemplates = (alertmanager_config: Record<string, any>) => _.get(alertmanager_config, 'template_files') as Record<string, any>;

const createContactPointsAndNotificationPolicy = async (metadata: Record<string, any>) => {
    const { receiver, notificationPolicy } = metadata;
    const config = await getAlertManagerConfig();
    const existingReceivers = getReceivers(config) || [];
    const existingRoutes = getRoutes(config) || [];
    const existingTemplates = getTemplates(config) || {};
    _.set(config, 'alertmanager_config.receivers', [...existingReceivers, receiver]);
    _.set(config, 'alertmanager_config.route.routes', [...existingRoutes, notificationPolicy]);
    _.set(config, 'template_files', { ...existingTemplates, ...defaultTemplates });
    return updateAlertManagerConfig(config);
};

const removeReceiverAndNotificationPolicy = (payload: Record<string, any>, alertManagerConfig: Record<string, any>) => {
    const { name } = payload;
    const clonedAlertManagerConfig = _.cloneDeep(alertManagerConfig);
    const existingReceivers = getReceivers(clonedAlertManagerConfig) || [];
    const existingRoutes = getRoutes(clonedAlertManagerConfig) || [];
    _.remove(existingRoutes, route => _.get(route, 'receiver') === name);
    _.remove(existingReceivers, receiver => _.get(receiver, 'name') === name);
    return clonedAlertManagerConfig;
};

export {
    generateChannelConfig,
    getAlertManagerConfig,
    updateAlertManagerConfig,
    getReceivers,
    getRoutes,
    getTemplates,
    createContactPointsAndNotificationPolicy,
    removeReceiverAndNotificationPolicy
}