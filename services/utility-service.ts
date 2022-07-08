import * as _ from "lodash";

class UtilityService {
    private static instance: UtilityService;

    public static getSingletonInstance(): UtilityService {
        return UtilityService.instance ||= new UtilityService();
    }

    public prepareDataForBigQuery(schema: Array<string>, data: { [key: string]: any }) {
        return _.pick(data, schema);
    }
}

export default UtilityService.getSingletonInstance();
