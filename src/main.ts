import { parseString } from "@fast-csv/parse";
import { PrismaClient } from "@prisma/client";
import { PrismaClientKnownRequestError } from "@prisma/client/runtime/library";
import "dotenv/config";
import { readFileSync, writeFileSync } from "node:fs";
import { files } from "./files";

interface Row {
  Order: string;
  Date: string;
  Narration: string;
  "Value Dat": string;
  "Debit Amount": string;
  "Credit Amount": string;
  "Chq/Ref Number": string;
  "Closing Balance": string;
}

const parseDate = (longDate: string): Date => {
  const [date, month, year] = longDate.split("/").map(Number);
  return new Date(2000 + year, month - 1, date);
};

const getRefinedCsv = (files: string[]): string => {
  let count = 0;
  const headers = [
    "Order",
    "Date",
    "Narration",
    "Value Date",
    "Debit Amount",
    "Credit Amount",
    "Chq/Ref Number",
    "Closing Balance",
  ];
  const rows = files.flatMap((file) => {
    const data = readFileSync(file, "utf-8");
    const lines = data.trim().split("\n").splice(1);
    return lines.map((line) => {
      const cells = `${++count},${line.trim()}`.split(",");
      cells[2] = `"${[
        cells[2],
        ...cells.splice(3, cells.length - headers.length),
      ]
        .join(",")
        .trim()}"`;
      return cells.map((str) => str.trim()).join(",");
    });
  });
  return [headers.join(","), ...rows].join("\n");
};

const writeCsv = (csv: string): void => {
  const path = `file.csv`;
  writeFileSync(path, csv);
};

const writeToDb = async (csv: string): Promise<void> => {
  const prisma = new PrismaClient();
  parseString(csv, { headers: true }).on("data", async (row: Row) => {
    try {
      await prisma.transaction.create({
        data: {
          order: Number(row.Order),
          date: parseDate(row.Date),
          description: row.Narration,
          amount: Number(row["Credit Amount"]) - Number(row["Debit Amount"]),
          closingBalance: Number(row["Closing Balance"]),
          reference: row["Chq/Ref Number"],
        },
      });
      console.log(`Transaction ${row.Order} created`);
    } catch (err) {
      if (err instanceof PrismaClientKnownRequestError) {
        console.log(`Transaction ${row.Order} already exists, skipping.`);
      }
    }
  });
};

async function main() {
  const csv = getRefinedCsv(files);
  writeCsv(csv);
  await writeToDb(csv);
}

main();
