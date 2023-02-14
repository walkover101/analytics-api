import { Request, Response, NextFunction } from 'express';
import jwt from 'jsonwebtoken';
import admin from '../firebase';
import logger from '../logger/logger';
import { CacheContainer } from 'node-ts-cache';
import { MemoryStorage } from 'node-ts-cache-storage-memory';

const cache = new CacheContainer(new MemoryStorage());
export enum Auth {
    TOKEN = "token",
    API_KEY = "apiKey"
}

export function auth(method: Auth[] = [Auth.TOKEN]) {
    // apiKey Auth
    if (method.some(m => m == Auth.API_KEY)) {
        return apiKeyAuth;
    } else {
        return tokenAuth;
    }

}

async function tokenAuth(req: Request, res: Response, next: NextFunction) {
    let token = req.header('Authorization')?.replace('Bearer ', '');
    console.log(token);

    if (!token) {
        logger.error("Token not found");
        return res.status(401).send("Token not found");
    }

    try {
        const secret = await getJWTSecret();
        const decoded: any = jwt.verify(token, secret);
        let companyId = decoded?.companyId as string;
        if (!companyId) companyId = decoded?.company?.id;
        logger.info(`companyId : ${companyId}`);
        req.query.companyId = companyId;
    } catch (error) {
        const email = await getFirebaseEmail(token);
        if (!email) return res.status(401).send("Invalid Token");
        if (!await isAdmin(email)) return res.status(403).send("Forbidden");
    }
    next();
}

async function apiKeyAuth(req: Request, res: Response, next: NextFunction) {
    // apiKey Auth
    const apiKey = req.header('x-api-key');
    if (!apiKey) {
        logger.error("API Key not found");
        return res.status(401).send("API Key not found");
    }
    if (apiKey != process.env.API_KEY) {
        logger.error("Invalid API Key");
        return res.status(401).send("Invalid API Key");
    }

    next();
}

async function getFirebaseEmail(token: string) {
    try {
        const payload = await admin.auth().verifyIdToken(token, false);
        return payload?.email;
    } catch (error) {
        logger.error(error);
        return null;
    }
}

async function getJWTSecret() {
    let jwtSecret;

    try {
        const ref = admin.database().ref("/jwt_secret");
        jwtSecret = await cache.getItem<string>('secret');

        if (!jwtSecret) {
            jwtSecret = (await ref.get()).val();
            await cache.setItem('secret', jwtSecret, { ttl: 900 });
        }
    } catch (error) {
        logger.error(error);
    }

    return jwtSecret;
}

async function isAdmin(email?: string) {
    if (!email) return false;
    let admins;

    try {
        const ref = admin.database().ref("/users");
        admins = await cache.getItem<string[]>('adminList');

        if (!admins || !admins.length) {
            admins = (await ref.get()).val();
            await cache.setItem('adminList', admins, { ttl: 1 });
        }
    } catch (error) {
        logger.error(error);
    }

    return Array.isArray(admins) && admins.includes(email);
}

