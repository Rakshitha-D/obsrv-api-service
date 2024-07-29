type IPromiseAny = Promise<any>;

interface IAlert {
    publishAlert(payload: Record<string, any>): IPromiseAny;
    getAlerts(payload: Record<string, any>): IPromiseAny;
    deleteAlert(payload: Record<string, any>): IPromiseAny;
}

interface INotificationChannel {
    createNotificationChannel(payload: Record<string, any>): IPromiseAny
    testNotificationChannel(payload: Record<string, any>, message: string): IPromiseAny;
    updateNotificationChannel(payload: Record<string, any>): IPromiseAny;
}

interface IManager extends IAlert, INotificationChannel {
    name: string
}

export interface IGrafana extends IManager {
    name: "grafana"
}

export interface IPrometheus extends IManager {
    name: "prometheus"
}

interface IChannelService {
    generateConfigPayload: (payload: Record<string, any>) => Record<string, any>,
    testChannel: (payload: Record<string, any>) => Promise<any>
}

export interface IChannelConfig {
    name: string,
    service: IChannelService
}

