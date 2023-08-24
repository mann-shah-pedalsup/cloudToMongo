const express = require("express");
const mongoose = require("mongoose");
const { BigQuery } = require("@google-cloud/bigquery");
const Station = require("./model/station");
const snowflake = require("snowflake-sdk");

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

app.get("/metadata", async (req, res) => {
  try {
    console.log("Request started");
    const bigquery = new BigQuery({
      keyFilename: "./" + process.env.KET_FILE_NAME,
      projectId: process.env.PROJECT_ID,
    });
    const dataset = bigquery.dataset(process.env.DATABASE_NAME);

    const [datasetMetadata] = await dataset.getMetadata();
    const [tablesData] = await dataset.getTables();
    const tableMetadata = await Promise.all(
      tablesData.map(async (table) => {
        const [metadata] = await table.getMetadata();
        return metadata;
      })
    );

    const metadata = {
      ...datasetMetadata,
      tables: tableMetadata,
    };
    const collection = mongoose.connection.db.collection("metadata");
    await collection.insertOne(metadata);
    console.log(collection);
    res.status(200).send({ data: "Data added successfully" });
  } catch (error) {}
});

app.get("/snowflakedata", async (req, res) => {
  try {
    console.log("Request started");

    const connectionConfig = {
      account: process.env.SNOWFLAKE_ACC,
      username: process.env.SNOWFLAKE_USERNAME,
      password: process.env.SNOWFLAKE_PASSWORD,
      warehouse: process.env.SNOWFLAKE_WAREHOUSE,
      database: process.env.SNOWFLAKE_DATABASE,
      schema: process.env.SNOWFLAKE_SCHEMA,
    };

    const connection = snowflake.createConnection(connectionConfig);

    // connection.connect(async (err, conn) => {
    //   if (err) {
    //     console.error("Error connecting to Snowflake:", err);
    //     return;
    //   }
    //   console.log("Connected to Snowflake");

    //   // Fetch database metadata for SNOWFLAKE_SAMPLE_DATA
    //   const databaseStatement = `
    //     SHOW DATABASES LIKE 'SNOWFLAKE_SAMPLE_DATA'
    //   `;
    //   let metadata = {};
    //   conn.execute({
    //     sqlText: databaseStatement,
    //     complete: function (err, stmt, databaseRows) {
    //       if (err) {
    //         console.error(
    //           "Failed to execute statement for database metadata due to the following error: " +
    //             err.message
    //         );
    //         conn.destroy();
    //         return;
    //       } else {
    //         const tableStatement = `SELECT * FROM "SNOWFLAKE_SAMPLE_DATA".INFORMATION_SCHEMA.TABLES`;

    //         conn.execute({
    //           sqlText: tableStatement,
    //           complete: function (err, stmt, tableRows) {
    //             if (err) {
    //               console.error(
    //                 "Failed to execute statement for table metadata due to the following error: " +
    //                   err.message
    //               );
    //             } else {
    //               // Process metadata for the tables
    //               metadata = {
    //                 ...databaseRows,
    //                 table: tableRows,
    //               };
    //               const collection = mongoose.connection.db.collection("snowflakes");
    //               await collection.insertOne(metadata);
    //               res.status(200).send({ data: "Data added successfully" });
    //             }

    //             conn.destroy();
    //           },
    //         });
    //       }
    //     },
    //   });
    // });
    connection.connect(async (err, conn) => {
      if (err) {
        console.error("Error connecting to Snowflake:", err);
        return;
      }
      console.log("Connected to Snowflake");

      try {
        // Fetch database metadata for SNOWFLAKE_SAMPLE_DATA
        const databaseStatement = `
          SHOW DATABASES LIKE 'SNOWFLAKE_SAMPLE_DATA'
        `;

        const databaseRows = await new Promise((resolve, reject) => {
          conn.execute({
            sqlText: databaseStatement,
            complete: (err, stmt, rows) => {
              if (err) {
                console.error(
                  "Failed to execute statement for database metadata due to the following error: " +
                    err.message
                );
                reject(err);
              } else {
                resolve(rows);
              }
            },
          });
        });

        // Fetch all available table metadata for tables in SNOWFLAKE_SAMPLE_DATA
        const tableStatement = `
          SELECT * FROM "SNOWFLAKE_SAMPLE_DATA".INFORMATION_SCHEMA.TABLES
        `;

        const tableRows = await new Promise((resolve, reject) => {
          conn.execute({
            sqlText: tableStatement,
            complete: (err, stmt, rows) => {
              if (err) {
                console.error(
                  "Failed to execute statement for table metadata due to the following error: " +
                    err.message
                );
                reject(err);
              } else {
                resolve(rows);
              }
            },
          });
        });

        // Process metadata for the tables
        const metadata = {
          ...databaseRows[0],
          table: tableRows,
        };

        const collection = mongoose.connection.db.collection("snowflakes");
        await collection.insertOne(metadata);

        res.status(200).send({ data: "Data added successfully" });
      } catch (error) {
        console.error("An error occurred:", error);
        res.status(500).send({ error: "An error occurred" });
      } finally {
        conn.destroy();
      }
    });
  } catch (error) {
    console.error("An error occurred:", error);
    res.status(500).send({ error: "An error occurred" });
  }
});
