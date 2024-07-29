import axios from 'axios';
import _ from 'lodash';
import CONSTANTS from '../../../constants'
import { IChannelConfig } from '../../../../../types/AlertModels';

const generateConfigPayload = (payload: Record<string, any>): Record<string, any> => {
    const { type, config, name } = payload;
    const { webhookUrl, labels = [[`notificationChannel_${name}_${type.toLowerCase()}`, "=", "true"]] } = config;

    return {
        notificationPolicy: {
            receiver: name,
            object_matchers: labels
        },
        receiver: {
            name,
            grafana_managed_receiver_configs: [
                {
                    settings: {
                        title: "{{ template \"slack_title\" . }}",
                        text: "{{ template \"slack_body\" . }}"
                    },
                    secureSettings: {
                        token: "",
                        url: webhookUrl
                    },
                    type,
                    name,
                    disableResolveMessage: false
                }
            ]
        }
    }
}

const testChannel = (payload: Record<string, any>): Promise<any> => {
    const { config, message } = payload;
    const { webhookUrl } = config;
    if (!webhookUrl) throw new Error(CONSTANTS.WEBHOOK_MISSING);
    return axios.post(webhookUrl, { text: message }, { headers: { "Content-Type": "application/json" } })
}

const service: IChannelConfig = {
    name: "slack",
    service: {
        generateConfigPayload,
        testChannel
    }
}

export default service