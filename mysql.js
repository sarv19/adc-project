const fs = require('fs');
const mysql = require('mysql2');
const csvParser = require('csv-parser');

const connection = mysql.createConnection({
    host: 'localhost',
    user: 'root',
    password: '123123',
    database: 'mysql',
    port: 3306
});

const csvFilePath = './Hourly Weather Data in Gallipoli (2008-2021).csv';

function convertToDateTime(dateTimeString) {
    // Assuming the datetime format in the CSV is "DD.MM.YYYY HH:mm"
    const [date, time] = dateTimeString.split(' ');
    const [day, month, year] = date.split('.');
    const [hour, minute] = time.split(':');

    return `${year}-${month}-${day} ${hour}:${minute}:00`;
}


function insertDataIntoTable() {
    const data = [];

    fs.createReadStream(csvFilePath)
        .pipe(csvParser({ separator: ';' }))
        .on('data', (row) => {
            data.push(row);
        })
        .on('end', () => {
            
            const insertQuery = `
        INSERT INTO weather_data (DateTime, Temperature, SunshineDuration, ShortwaveRadiation, RelativeHumidity, MeanSeaLevelPressure, SoilTemperature, SoilMoisture, WindSpeed, WindDirection)
        VALUES ?
      `;

            const startTime = performance.now();

            const values = data.map((row) => [
                convertToDateTime(row.DateTime),
                parseFloat(row.Temperature),
                parseFloat(row.SunshineDuration),
                parseFloat(row.ShortwaveRadiation),
                parseFloat(row.RelativeHumidity),
                parseFloat(row.MeanSeaLevelPressure),
                parseFloat(row.SoilTemperature),
                parseFloat(row.SoilMoisture),
                parseFloat(row.WindSpeed),
                parseFloat(row.WindDirection),
            ]);

            connection.query(insertQuery, [values], (err, result) => {
                if (err) {
                    console.error('Error inserting data:', err);
                } else {
                    const endTime = performance.now();
                    const timeTaken = endTime - startTime;
                    console.log(`Data inserted successfully! Time taken: ${timeTaken.toFixed(2)} milliseconds`);
                    closeConnection();
                }
            });
        });
}

function closeConnection() {
    connection.end((err) => {
        if (err) {
            console.error('Error closing connection:', err);
        } else {
            console.log('Connection closed.');
        }
    });
}

// insertDataIntoTable();

async function getAverageTemperatureMySQL() {
  
    try {
      console.log('Connected to MySQL successfully!');
  
      // Write the query to calculate the average temperature
      const query = `
        SELECT AVG(Temperature) AS average_temperature
        FROM weather_data
        WHERE DateTime BETWEEN '2020-02-01 00:00:00' AND '2020-05-31 23:59:59'
      `;
      const startTime = performance.now();
      // Execute the query
      const [rows] = await connection.execute(query);
      const endTime = performance.now();
      const timeTaken = endTime - startTime;
      // Display the result
      console.log(`Time taken: ${timeTaken.toFixed(2)} milliseconds`);
      console.log('Average temperature between Feb 2020 and May 2020:');
      console.log(rows[0].average_temperature);
    } catch (err) {
      console.error('Error executing query:', err);
    } finally {
      // Close the connection after the operation is completed
      await connection.end();
    }
  }
  
  getAverageTemperatureMySQL();
