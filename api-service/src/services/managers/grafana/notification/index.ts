import _ from "lodash";
import { getChannelService } from './channels';
import { createContactPointsAndNotificationPolicy, generateChannelConfig, getAlertManagerConfig, removeReceiverAndNotificationPolicy, updateAlertManagerConfig } from "./helpers";

const updateNotificationChannel = async (payload: Record<string, any>) => {
    const alertManagerConfig = await getAlertManagerConfig();
    const updatedAlertManagerConfig = removeReceiverAndNotificationPolicy(payload, alertManagerConfig);
    return updateAlertManagerConfig(updatedAlertManagerConfig);
};

const createNotificationChannel = (payload: Record<string, any>) => {
    const { type, config, name } = payload;
    const { notificationPolicy, receiver } = generateChannelConfig({ type, config, name });
    return createContactPointsAndNotificationPolicy({ notificationPolicy, receiver });
};

const testNotificationChannel = async (payload: Record<string, any>) => {
    const { type } = payload;
    const channel = getChannelService(type);
    return channel.service.testChannel(payload);
};

export {
    updateNotificationChannel,
    createNotificationChannel,
    testNotificationChannel
}