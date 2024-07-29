import path from 'path';
import { scrapModules } from '../../../../../services/fs';
import { IChannelConfig } from '../../../../../types/AlertModels';

const channels = scrapModules<IChannelConfig>(__dirname, path.basename(__filename));

export const getChannelService = (channelName: string) => {
    const channel = channels.get(channelName.toLowerCase());
    if (!channel) throw new Error('invalid channel');
    return channel;
}