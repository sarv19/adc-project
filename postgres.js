const fs = require('fs');
const { Client, Pool } = require('pg');
const csvParser = require('csv-parser');

const client = new Client({
    user: 'postgres',
    password: '123123', // Replace with your PostgreSQL password
    host: 'localhost',
    port: 5432,
    database: 'postgres',
});

const csvFilePath = './Hourly Weather Data in Gallipoli (2008-2021).csv';

const createTableQuery = `
  CREATE TABLE IF NOT EXISTS weather_data (
    "DateTime" TIMESTAMP,
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

async function createTable() {
    try {
        await client.connect();
        await client.query(createTableQuery);
        console.log('Table created or already exists!');
    } catch (err) {
        console.error('Error creating table:', err);
    }
}

function insertDataIntoTable() {
    const data = [];

    fs.createReadStream(csvFilePath)
        .pipe(csvParser({ separator: ';' }))
        .on('data', (row) => {
            data.push(row);
        })
        .on('end', async () => {
            try {
                // await client.connect();
                const startTime = performance.now();

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

                    const insertQuery = `
            INSERT INTO weather_data ("DateTime", "Temperature", "SunshineDuration", "ShortwaveRadiation", "RelativeHumidity", "MeanSeaLevelPressure", "SoilTemperature", "SoilMoisture", "WindSpeed", "WindDirection")
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
          `;

                    await client.query(insertQuery, values);
                }
                const endTime = performance.now();
                const timeTaken = endTime - startTime;
                console.log(`Data inserted successfully! Time taken: ${timeTaken.toFixed(2)} milliseconds`);
            } catch (err) {
                console.error('Error inserting data:', err);
            } finally {
                client.end();
            }
        });
}

function convertToDateTime(dateTimeString) {
    // Assuming the datetime format in the CSV is "DD.MM.YYYY HH:mm"
    const [date, time] = dateTimeString.split(' ');
    const [day, month, year] = date.split('.');
    const [hour, minute] = time.split(':');

    return `${year}-${month}-${day} ${hour}:${minute}:00`;
}

// async function main() {
//   await createTable();
//   insertDataIntoTable();
// }

// main();

async function getAverageTemperaturePostgres() {
    const pool = new Pool({
        user: 'postgres',
        password: '123123', // Replace with your PostgreSQL password
        host: 'localhost',
        port: 5432,
        database: 'postgres',
});

    try {
        console.log('Connected to PostgreSQL successfully!');
        // Start the timer
        const startTime = performance.now();

        // Write the query to calculate the average temperature
        const query = `
        SELECT AVG(Temperature) AS average_temperature
        FROM weather_data
        WHERE "DateTime" BETWEEN '2020-02-01 00:00:00' AND '2020-05-31 23:59:59'
      `;

        // Execute the query
        const { rows } = await pool.query(query);
        // End the timer
        const endTime = performance.now();
        const timeTaken = endTime - startTime;
        // Display the result
        console.log('Average temperature between Feb 2020 and May 2020:');
        console.log(rows[0].average_temperature);
        console.log(`Time taken: ${timeTaken.toFixed(2)} milliseconds`);
    } catch (err) {
        console.error('Error executing query:', err);
    } finally {
        // Close the pool after the operation is completed
        await pool.end();
    }
}

getAverageTemperaturePostgres();
