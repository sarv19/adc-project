const fs = require('fs');
const { Client } = require('pg');
const csvParser = require('csv-parser');

const dbConfig = {
  user: 'postgres', // Replace with your TimescaleDB username
  host: 'localhost',
  database: 'timescaledb', // Replace with your TimescaleDB database name
  password: '123123', // Replace with your TimescaleDB password
  port: 5432, // Replace with your TimescaleDB port
};

const client = new Client(dbConfig);

const csvFilePath = './Hourly Weather Data in Gallipoli (2008-2021) short.csv'; // Replace with the path to your CSV file
const tableName = 'weather_data'; // Replace with the table name where you want to insert data

async function connectAndInsertData() {
  try {
    await client.connect();
    console.log('Connection to TimescaleDB successful!');

    const createTableQuery = `
      CREATE TABLE IF NOT EXISTS ${tableName} (
        "DateTime" TIMESTAMPTZ PRIMARY KEY,
        "Temperature" FLOAT,
        "SunshineDuration" FLOAT,
        "ShortwaveRadiation" FLOAT,
        "RelativeHumidity" FLOAT,
        "MeanSeaLevelPressure" FLOAT,
        "SoilTemperature" FLOAT,
        "SoilMoisture" FLOAT,
        "WindSpeed" FLOAT,
        "WindDirection" FLOAT
      )
    `;

    await client.query(createTableQuery);

    const data = [];

    fs.createReadStream(csvFilePath)
      .pipe(csvParser({ separator: ';' }))
      .on('data', (row) => {
        data.push(row);
      })
      .on('end', async () => {
        const startTime = Date.now();

        const insertQuery = `
          INSERT INTO ${tableName} ("DateTime", "Temperature", "SunshineDuration", "ShortwaveRadiation", "RelativeHumidity", "MeanSeaLevelPressure", "SoilTemperature", "SoilMoisture", "WindSpeed", "WindDirection")
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        `;

        for (const row of data) {
          const values = [
            convertToDateTime(row['DateTime']),
            parseFloat(row['Temperature']),
            parseFloat(row['SunshineDuration']),
            parseFloat(row['ShortwaveRadiation']),
            parseFloat(row['RelativeHumidity']),
            parseFloat(row['MeanSeaLevelPressure']),
            parseFloat(row['SoilTemperature']),
            parseFloat(row['SoilMoisture']),
            parseFloat(row['WindSpeed']),
            parseFloat(row['WindDirection']),
          ];
          await client.query(insertQuery, values);
        }

        const endTime = Date.now();
        const timeTaken = endTime - startTime;
        console.log(`Data inserted successfully! Time taken: ${timeTaken.toFixed(2)} milliseconds`);
        await client.end();
      });
  } catch (err) {
    console.error('Error connecting to TimescaleDB:', err);
    client.end();
  }
}

function convertToDateTime(dateTimeString) {
  // Assuming the datetime format in the CSV is "DD.MM.YYYY HH:mm"
  const [date, time] = dateTimeString.split(' ');
  const [day, month, year] = date.split('.');
  const [hour, minute] = time.split(':');

  return `${year}-${month}-${day}T${hour}:${minute}:00Z`;
}

async function main() {
  await connectAndInsertData();
}

main();
