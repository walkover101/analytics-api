const express = require('express');
import { v4 as uuidv4 } from 'uuid';
import { Request, Response } from 'express';
import z from "zod";
import { DateTime, MonthNumbers } from 'luxon';
import { upload } from '../database/gcs-storage';
const router = express.Router();
import { generatePDF } from '../services/report/pdf-report';
const monthSchema = z.number().min(1).max(12);
router.route('/').get(async (req: Request, res: Response) => {
    // Generate a unique id for the file name
    // res.setHeader('Content-Length', stat.size);
    // res.setHeader('Content-Type', 'application/pdf');
    // res.setHeader('Content-Disposition', 'attachment; filename=quote.pdf');
    // res.send("Working")
    const companyId = req.query.companyId;
    let month = parseInt(req.query.month as string);
    let year = parseInt(req.query.year as string) || DateTime.utc().year;
    monthSchema.parse(month);
    const startDate = DateTime.utc().set({ year: year, month: month as any, day: 1, hour: 0, minute: 0, second: 0, millisecond: 0 });
    const endDate = DateTime.utc().set({ year: year, month: month as any, day: 1, hour: 0, minute: 0, second: 0, millisecond: 0 }).plus({ month: 1 });
    // console.log(startDate.toISO(), endDate.toISO());
    const link = await upload(await generatePDF(companyId as string, startDate, endDate), 'pdf_report', `report-${uuidv4()}.pdf`) as string[];
    // res.send(await generatePDF(companyId as string, startDate, endDate))
    res.send(link[0]);
});

export default router;