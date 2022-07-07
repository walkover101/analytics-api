class UtilityService {
    private static instance: UtilityService;

    public static getSingletonInstance(): UtilityService {
        UtilityService.instance ||= new UtilityService();
        return UtilityService.instance;
    }

    public trimData(schema: Array<string>, data: { [key: string]: any }) {
        const output: { [key: string]: any } = {};

        schema.forEach((key: string) => {
            output[key as string] = data[key as string];
        })

        return output;
    }
}

export default UtilityService.getSingletonInstance();
