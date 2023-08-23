const express = require("express");
const mongoose = require("mongoose");
const { BigQuery } = require("@google-cloud/bigquery");
const Station = require("./model/station");
require("dotenv").config();
const app = express();
app.use(express.json());

mongoose
  .connect(process.env.MONGO_URL)
  .then(() => {
    console.log("Connected to MongoDB");
    app.listen(process.env.PORT, () => {
      console.log("Port on 8080");
    });
  })
  .catch((err) => {
    console.log(err);
  });

app.get("/upload", async (req, res) => {
  try {
    const bigquery = new BigQuery({
      keyFilename: "./" + process.env.KET_FILE_NAME,
      projectId: process.env.PROJECT_ID,
    });
    const query = `SELECT * FROM ${process.env.PROJECT_ID}.${process.env.DATABASE_NAME}.${process.env.TABLE_NAME}`;
    const options = {
      query: query,
      location: "US",
    };
    const [rows] = await bigquery.query(options);
    // console.log(rows);
    const transformedData = rows.map((row) => {
      row.last_reported = new Date(row.last_reported.value);
      return new Station(row);
    });
    await Station.insertMany(transformedData);
    res.status(200).send({ data: "Data added successfully" });
  } catch (error) {
    console.log(error);
  }
});
