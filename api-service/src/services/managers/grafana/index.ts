import * as alertFunctions from './alert'
import * as notificationFunctions from './notification';
import * as silenceFunctions from './silences';

const service = { name: "grafana", ...alertFunctions, ...notificationFunctions, ...silenceFunctions };
export default service