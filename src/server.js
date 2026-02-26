const express = require("express");
const uuidv4 = require("uuid").v4;
const createCsvWriter = require("csv-writer").createObjectCsvWriter;
const { fullExport } = require("./exportService");
const { Pool } = require("pg");
const app = express();
const pool = new Pool({
  host: "db",
  user: "user",
  password: "password",
  database: "mydatabase",
  port: 5432,
});
app.get("/health", (req, res) => {
  res.status(200).json({
    status: "ok",
    timestamp: new Date().toISOString()
  });
});

app.post("/exports/full", async (req, res) => {
  const consumerId = req.headers["x-consumer-id"];

  if (!consumerId) {
    return res.status(400).json({ error: "X-Consumer-ID required" });
  }

  const jobId = uuidv4();
  const filename = `full_${consumerId}_${Date.now()}.csv`;

  setImmediate(() => {
    fullExport(consumerId, filename);
  });

  res.status(202).json({
    jobId,
    status: "started",
    exportType: "full",
    outputFilename: filename
  });
});
app.get("/users", async (req, res) => {
  try {
    const result = await pool.query(
      "SELECT * FROM users WHERE is_deleted = FALSE"
    );
    res.json(result.rows);
  } catch (err) {
    console.error(err);
    res.status(500).send("Error fetching users");
  }
});
app.get("/exports/watermark", async (req, res) => {
  try {
    const consumerId = req.headers["x-consumer-id"];

    if (!consumerId) {
      return res.status(400).json({ error: "X-Consumer-ID header missing" });
    }

    const result = await pool.query(
      "SELECT last_exported_at FROM watermarks WHERE consumer_id = $1",
      [consumerId]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ message: "No watermark found" });
    }

    res.json({
      consumerId,
      lastExportedAt: result.rows[0].last_exported_at,
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Internal server error" });
  }
});
app.post("/exports/full", async (req, res) => {
  try {
    const consumerId = req.headers["x-consumer-id"];

    if (!consumerId) {
      return res.status(400).json({ error: "X-Consumer-ID header missing" });
    }

    const jobId = Date.now();
    const filename = `full_${consumerId}_${jobId}.csv`;
    const filepath = `/app/output/${filename}`;

    res.status(202).json({
      jobId,
      status: "started",
      exportType: "full",
      outputFilename: filename,
    });

    // Run async export job
    (async () => {
      console.log(
        JSON.stringify({
          event: "Export job started",
          jobId,
          consumerId,
          exportType: "full",
        })
      );

      const users = await pool.query(
        "SELECT * FROM users WHERE is_deleted = false"
      );

      const csvWriter = createCsvWriter({
        path: filepath,
        header: [
          { id: "id", title: "id" },
          { id: "name", title: "name" },
          { id: "email", title: "email" },
          { id: "created_at", title: "created_at" },
          { id: "updated_at", title: "updated_at" },
          { id: "is_deleted", title: "is_deleted" },
        ],
      });

      await csvWriter.writeRecords(users.rows);

      const maxUpdatedAt = await pool.query(
        "SELECT MAX(updated_at) FROM users WHERE is_deleted = false"
      );

      await pool.query(
        `INSERT INTO watermarks (consumer_id, last_exported_at, updated_at)
         VALUES ($1, $2, NOW())
         ON CONFLICT (consumer_id)
         DO UPDATE SET last_exported_at = $2, updated_at = NOW()`,
        [consumerId, maxUpdatedAt.rows[0].max]
      );

      console.log(
        JSON.stringify({
          event: "Export job completed",
          jobId,
          rowsExported: users.rows.length,
        })
      );
    })();
  } catch (err) {
    console.error(err);
  }
});
app.post("/exports/incremental", async (req, res) => {
  try {
    const consumerId = req.headers["x-consumer-id"];

    if (!consumerId) {
      return res.status(400).json({ error: "X-Consumer-ID header missing" });
    }

    const jobId = Date.now();
    const filename = `incremental_${consumerId}_${jobId}.csv`;
    const filepath = `/app/output/${filename}`;

    res.status(202).json({
      jobId,
      status: "started",
      exportType: "incremental",
      outputFilename: filename,
    });

    (async () => {
      console.log(
        JSON.stringify({
          event: "Export job started",
          jobId,
          consumerId,
          exportType: "incremental",
        })
      );

      const watermark = await pool.query(
        "SELECT last_exported_at FROM watermarks WHERE consumer_id = $1",
        [consumerId]
      );

      if (watermark.rows.length === 0) {
        console.log("No watermark found. Run full export first.");
        return;
      }

      const lastExportedAt = watermark.rows[0].last_exported_at;

      const users = await pool.query(
        "SELECT * FROM users WHERE updated_at > $1 AND is_deleted = false",
        [lastExportedAt]
      );

      if (users.rows.length === 0) {
        console.log("No new updates found.");
        return;
      }

      const csvWriter = createCsvWriter({
        path: filepath,
        header: [
          { id: "id", title: "id" },
          { id: "name", title: "name" },
          { id: "email", title: "email" },
          { id: "created_at", title: "created_at" },
          { id: "updated_at", title: "updated_at" },
          { id: "is_deleted", title: "is_deleted" },
        ],
      });

      await csvWriter.writeRecords(users.rows);

      const maxUpdatedAt = await pool.query(
        "SELECT MAX(updated_at) FROM users WHERE updated_at > $1",
        [lastExportedAt]
      );

      await pool.query(
        `UPDATE watermarks 
         SET last_exported_at = $1, updated_at = NOW()
         WHERE consumer_id = $2`,
        [maxUpdatedAt.rows[0].max, consumerId]
      );

      console.log(
        JSON.stringify({
          event: "Export job completed",
          jobId,
          rowsExported: users.rows.length,
        })
      );
    })();
  } catch (err) {
    console.error(err);
  }
});
app.post("/exports/delta", async (req, res) => {
  try {
    const consumerId = req.headers["x-consumer-id"];

    if (!consumerId) {
      return res.status(400).json({ error: "X-Consumer-ID header missing" });
    }

    const jobId = Date.now();
    const filename = `delta_${consumerId}_${jobId}.csv`;
    const filepath = `/app/output/${filename}`;

    res.status(202).json({
      jobId,
      status: "started",
      exportType: "delta",
      outputFilename: filename,
    });

    (async () => {
      console.log(JSON.stringify({
        event: "Export job started",
        jobId,
        consumerId,
        exportType: "delta",
      }));

      const watermark = await pool.query(
        "SELECT last_exported_at FROM watermarks WHERE consumer_id = $1",
        [consumerId]
      );

      if (watermark.rows.length === 0) {
        console.log("No watermark found.");
        return;
      }

      const lastExportedAt = watermark.rows[0].last_exported_at;

      const users = await pool.query(
        "SELECT * FROM users WHERE updated_at > $1",
        [lastExportedAt]
      );

      if (users.rows.length === 0) {
        console.log("No changes found.");
        return;
      }

      const records = users.rows.map((user) => {
        let operation = "UPDATE";

        if (user.is_deleted) operation = "DELETE";
        else if (user.created_at.getTime() === user.updated_at.getTime())
          operation = "INSERT";

        return { operation, ...user };
      });

      const csvWriter = createCsvWriter({
        path: filepath,
        header: [
          { id: "operation", title: "operation" },
          { id: "id", title: "id" },
          { id: "name", title: "name" },
          { id: "email", title: "email" },
          { id: "created_at", title: "created_at" },
          { id: "updated_at", title: "updated_at" },
          { id: "is_deleted", title: "is_deleted" },
        ],
      });

      await csvWriter.writeRecords(records);

      const maxUpdatedAt = await pool.query(
        "SELECT MAX(updated_at) FROM users WHERE updated_at > $1",
        [lastExportedAt]
      );

      await pool.query(
        `UPDATE watermarks
         SET last_exported_at = $1, updated_at = NOW()
         WHERE consumer_id = $2`,
        [maxUpdatedAt.rows[0].max, consumerId]
      );

      console.log(JSON.stringify({
        event: "Export job completed",
        jobId,
        rowsExported: records.length,
      }));
    })();
  } catch (err) {
    console.error(err);
  }
});
app.delete("/users/:id", async (req, res) => {
  const { id } = req.params;

  try {
    await pool.query(
      `UPDATE users
       SET is_deleted = TRUE,
           updated_at = NOW()
       WHERE id = $1`,
      [id]
    );

    res.send("User soft deleted");
  } catch (err) {
    console.error(err);
    res.status(500).send("Delete failed");
  }
});

app.listen(8080, () => {
  console.log("Server running on port 8080");
});