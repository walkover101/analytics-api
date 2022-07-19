import { Request, Response, NextFunction } from 'express';
import jwt from 'jsonwebtoken';
import admin from '../firebase';
import logger from '../logger/logger';
class Cache {
    private jwtSecret: string = "";
    private secretInvalidator: any = null;
    private adminList: Array<string> = [];
    private adminListInvalidator: any = null;
    getSecret(): string {
        return this.jwtSecret;
    }
    setSecret(secret: string, ttl: number = 900000) {
        this.jwtSecret = secret;
        clearTimeout(this.secretInvalidator);
        this.secretInvalidator = setTimeout(() => {
            this.jwtSecret = "";
        }, ttl);
    }
    getAdminList(): Array<string> {
        return this.adminList;
    }
    setAdminList(list: Array<string>, ttl: number = 900000) {
        this.adminList = list;
        clearTimeout(this.adminListInvalidator);
        this.adminListInvalidator = setTimeout(() => {
            this.adminList = [];
        }, ttl);
    }
    invalidate() {
        this.jwtSecret = "";
        this.adminList = [];
    }
}
const cache = new Cache();
export async function authenticate(req: Request, res: Response, next: NextFunction) {
    let token = req.header('Authorization')?.replace('Bearer ', '');
    if (token) {
        try {
            const secret = await getJWTSecret();
            const decoded: any = jwt.verify(token, secret);
            req.query.companyId = decoded?.companyId as string;
        } catch (error) {
            const payload = await admin.auth().verifyIdToken(token, false);
            if (!await isAdmin(payload?.email)) {
                return res.status(403).send("Forbidden");
            }
        }
        next();
    } else {
        logger.error("Token not found");
        return res.status(401).send("Token not found");
    }
}
async function getJWTSecret() {
    try {
        const ref = admin.database().ref("/jwt_secret");
        let jwtSecret = cache.getSecret();
        if (!jwtSecret) {
            jwtSecret = (await ref.get()).val();
            cache.setSecret(jwtSecret);
        }
        return jwtSecret;
    } catch (error) {
        logger.error(error);
        return "";
    }

}
async function isAdmin(email?: string) {
    if (!email) {
        return false;
    }
    const ref = admin.database().ref("/users");
    let admins = cache.getAdminList();
    if (!admins.length) {
        admins = (await ref.get()).val();
        cache.setAdminList(admins, 1000);
    }
    if (Array.isArray(admins)) {
        return admins.includes(email) ? true : false;
    } else {
        return false;
    }
}

