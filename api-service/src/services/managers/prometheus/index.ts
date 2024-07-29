import * as alertFunctions from './alert'
import * as notificationFunctions from './notification';
import * as silenceFunctions from './silences';

export default { ...alertFunctions, ...notificationFunctions, ...silenceFunctions }