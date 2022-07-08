class UtilityService {
    private static instance: UtilityService;

    public static getSingletonInstance(): UtilityService {
        return UtilityService.instance ||= new UtilityService();
    }

    public delay(time = 1000) {
        return new Promise((resolve) => {
            setTimeout(() => {
                return resolve(true);
            }, time)
        });
    }
}

export default UtilityService.getSingletonInstance();
