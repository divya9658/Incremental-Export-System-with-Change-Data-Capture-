const pool = require("./db");
const fs = require("fs");
const createCsvWriter = require("csv-writer").createObjectCsvWriter;

async function fullExport(consumerId, filename) {
  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    const result = await client.query(
      "SELECT * FROM users WHERE is_deleted = false"
    );

    const csvWriter = createCsvWriter({
      path: `output/${filename}`,
      header: [
        { id: "id", title: "id" },
        { id: "name", title: "name" },
        { id: "email", title: "email" },
        { id: "created_at", title: "created_at" },
        { id: "updated_at", title: "updated_at" },
        { id: "is_deleted", title: "is_deleted" }
      ]
    });

    await csvWriter.writeRecords(result.rows);

    const maxUpdated = await client.query(
      "SELECT MAX(updated_at) FROM users WHERE is_deleted = false"
    );

    const latestTimestamp = maxUpdated.rows[0].max;

    await client.query(
      `
      INSERT INTO watermarks (consumer_id, last_exported_at, updated_at)
      VALUES ($1, $2, NOW())
      ON CONFLICT (consumer_id)
      DO UPDATE SET
        last_exported_at = $2,
        updated_at = NOW()
      `,
      [consumerId, latestTimestamp]
    );

    await client.query("COMMIT");

    console.log(
      JSON.stringify({
        message: "Export completed",
        consumerId,
        rowsExported: result.rowCount
      })
    );
  } catch (err) {
    await client.query("ROLLBACK");
    console.error("Export failed:", err);
  } finally {
    client.release();
  }
}

module.exports = { fullExport };