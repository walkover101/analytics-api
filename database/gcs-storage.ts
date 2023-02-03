import { Storage } from '@google-cloud/storage';
import { CREDENTIALS } from './big-query-service';

const storage = new Storage({
    projectId: process.env.GCP_PROJECT_ID,
    credentials: CREDENTIALS
});

export function upload(buffer: Buffer, bucketName: string, destination: string) {
    return new Promise((resolve, reject) => {

        storage.bucket(bucketName).file(destination).save(buffer).then((value) => {
            // Get public URL for the file
            storage.bucket(bucketName).file(destination).getSignedUrl({
                action: 'read',
                expires: '03-09-2491'
            }).then((url) => {
                resolve(url);
            }).catch((err) => {
                reject(err);
            });
        })
    });
}


