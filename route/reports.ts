import express, { Request, Response } from 'express';
const router = express.Router();
router.route('/')
    .get((req: Request, res: Response) => {
        res.send("Work in progress...");
    })
export default router;