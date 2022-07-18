import { cert, initializeApp } from "firebase-admin/app";
import { Firestore, getFirestore } from "firebase-admin/firestore";

// import service account file (helps to know the firebase project details)
const serviceAccountCreds = require(process.env.FIREBASE_CREDENTIALS_FILE_PATH || './firebase-credentials.json');

const app = initializeApp({
  credential: cert(serviceAccountCreds),
  databaseURL: `https://msg91-analytics-test.firebaseio.com`
});

export const db: Firestore = getFirestore();
export default app;
