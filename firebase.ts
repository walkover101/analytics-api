import { cert, initializeApp } from "firebase-admin/app";
import { Firestore, getFirestore } from "firebase-admin/firestore";
import admin from 'firebase-admin';
// import service account file (helps to know the firebase project details)
const serviceAccountCreds = require(process.env.FIREBASE_CREDENTIALS_FILE_PATH || './firebase-credentials.json');

admin.initializeApp({
  credential: cert(serviceAccountCreds),
  databaseURL: process.env.FIREBASE_DB_URL
});

export const db: Firestore = admin.firestore();
export default admin;
