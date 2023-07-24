const fs = require('fs');
const csv = require('csv-parser');
const { InfluxDB, FieldType } = require('influx');
const { Client } = require('pg');
const mysql = require('mysql2');
const { MongoClient } = require('mongodb');

// Function to insert data into InfluxDB
async function insertDataInfluxDB(data) {
  const influx = new InfluxDB({
    host: 'localhost',
    username: 'influx',
    database: 'influxdb',
    password: '123123',
    port: '5444',
    schema: [
      {
        measurement: 'weather_data',
        fields: {
          Temperature: FieldType.FLOAT,
          Sunshine_Duration: FieldType.INTEGER,
          Shortwave_Radiation: FieldType.INTEGER,
          Relative_Humidity: FieldType.FLOAT,
          Mean_Sea_Level_Pressure: FieldType.FLOAT,
          Soil_Temperature: FieldType.FLOAT,
          Soil_Moisture: FieldType.FLOAT,
          Wind_Speed: FieldType.FLOAT,
          Wind_Direction: FieldType.FLOAT,
        },
        tags: ['location'],
      },
    ],
  });

  try {
    // Convert CSV data to InfluxDB data points
    const points = data.map((entry) => ({
      measurement: 'weather_data',
      fields: {
        Temperature: parseFloat(entry.Temperature),
        Sunshine_Duration: parseInt(entry['Sunshine Duration']),
        Shortwave_Radiation: parseInt(entry['Shortwave Radiation']),
        Relative_Humidity: parseFloat(entry['Relative Humidity']),
        Mean_Sea_Level_Pressure: parseFloat(entry['Mean Sea Level Pressure']),
        Soil_Temperature: parseFloat(entry['Soil Temperature']),
        Soil_Moisture: parseFloat(entry['Soil Moisture']),
        Wind_Speed: parseFloat(entry['Wind Speed']),
        Wind_Direction: parseFloat(entry['Wind Direction']),
      },
      tags: {}, // You can add tags here if needed
      timestamp: new Date(entry.DateTime), // Use the DateTime column for timestamp
    }));

    // Write the data points into InfluxDB
    await influx.writePoints(points);
    console.log('Data inserted successfully into InfluxDB!');
  } catch (error) {
    console.error('Error inserting data into InfluxDB:', error);
  }

//   await influx.writePoints(data, { database: 'influxdb', precision: 'ms' });
}

// // // Function to insert data into TimescaleDB
// // async function insertDataTimescaleDB(data) {
// //   const client = new Client({
// //     user: 'root',
// //     host: 'localhost',
// //     database: 'timescale/timescaledb:latest-pg15',
// //     password: '123123',
// //     port: 5999,
// //   });

// //   await client.connect();

// //   // Create a hypertable in TimescaleDB for efficient time-series storage
// //   await client.query(`
// //     CREATE TABLE IF NOT EXISTS weather_data (
// //       DateTime TIMESTAMP,
// //       Temperature FLOAT,
// //       Sunshine_Duration INT,
// //       Shortwave_Radiation INT,
// //       Relative_Humidity FLOAT,
// //       Mean_Sea_Level_Pressure FLOAT,
// //       Soil_Temperature FLOAT,
// //       Soil_Moisture FLOAT,
// //       Wind_Speed FLOAT,
// //       Wind_Direction FLOAT
// //     );

// //     SELECT create_hypertable('weather_data', 'DateTime');
// //   `);

// //   // Insert data into the hypertable
// //   const query = `
// //     INSERT INTO weather_data (DateTime, Temperature, Sunshine_Duration, Shortwave_Radiation, Relative_Humidity, Mean_Sea_Level_Pressure, Soil_Temperature, Soil_Moisture, Wind_Speed, Wind_Direction)
// //     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
// //   `;

// //   await client.query('BEGIN');
// //   try {
// //     for (const entry of data) {
// //       const values = [
// //         entry.DateTime,
// //         entry.Temperature,
// //         entry.Sunshine_Duration,
// //         entry.Shortwave_Radiation,
// //         entry.Relative_Humidity,
// //         entry.Mean_Sea_Level_Pressure,
// //         entry.Soil_Temperature,
// //         entry.Soil_Moisture,
// //         entry.Wind_Speed,
// //         entry.Wind_Direction,
// //       ];
// //       await client.query(query, values);
// //     }
// //     await client.query('COMMIT');
// //   } catch (error) {
// //     await client.query('ROLLBACK');
// //     throw error;
// //   } finally {
// //     client.end();
// //   }
// // }

// Function to insert data into PostgreSQL
async function insertDataPostgreSQL(data) {
  const client = new Client({
    user: 'postgres',
    password: '123123', // Replace with your PostgreSQL password
    host: 'localhost',
    port: 5432,
    database: 'postgres',
  });

  await client.connect();

  // Create the table in PostgreSQL for storing weather data
  await client.query(`
    CREATE TABLE IF NOT EXISTS weather_data (
      DateTime TIMESTAMP,
      Temperature FLOAT,
      Sunshine_Duration INT,
      Shortwave_Radiation INT,
      Relative_Humidity FLOAT,
      Mean_Sea_Level_Pressure FLOAT,
      Soil_Temperature FLOAT,
      Soil_Moisture FLOAT,
      Wind_Speed FLOAT,
      Wind_Direction FLOAT
    )
  `);

  // Insert data into the table
  const query = `
    INSERT INTO weather_data (DateTime, Temperature, Sunshine_Duration, Shortwave_Radiation, Relative_Humidity, Mean_Sea_Level_Pressure, Soil_Temperature, Soil_Moisture, Wind_Speed, Wind_Direction)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
  `;

  await client.query('BEGIN');
  try {
    for (const entry of data) {
      const values = [
        entry.DateTime,
        entry.Temperature,
        entry.Sunshine_Duration,
        entry.Shortwave_Radiation,
        entry.Relative_Humidity,
        entry.Mean_Sea_Level_Pressure,
        entry.Soil_Temperature,
        entry.Soil_Moisture,
        entry.Wind_Speed,
        entry.Wind_Direction,
      ];
      await client.query(query, values);
    }
    await client.query('COMMIT');
  } catch (error) {
    await client.query('ROLLBACK');
    throw error;
  } finally {
    client.end();
  }
}

// Function to insert data into MySQL
async function insertDataMySQL(data) {
  const connection = mysql.createConnection({
    host: 'localhost',
    user: 'root',
    password: '123123',
    database: 'mysql',
    port: 3306
  });

  connection.connect((err) => {
    if (err) throw err;

    function createTableIfNotExists() {
        connection.query(`
        CREATE TABLE IF NOT EXISTS weather_data (
            DateTime DATETIME,
            Temperature FLOAT,
            SunshineDuration FLOAT,
            ShortwaveRadiation FLOAT,
            RelativeHumidity FLOAT,
            MeanSeaLevelPressure FLOAT,
            SoilTemperature FLOAT,
            SoilMoisture FLOAT,
            WindSpeed FLOAT,
            WindDirection FLOAT
          )
        `, (err) => {
            if (err) {
                console.error('Error creating table:', err);
            } else {
                console.log('Table created or already exists!');
            }
        });
    }

    function convertToDateTime(dateTimeString) {
        // Assuming the datetime format in the CSV is "DD.MM.YYYY HH:mm"
        const [date, time] = dateTimeString?.split(' ');
        const [day, month, year] = date?.split('.');
        const [hour, minute] = time?.split(':');
      
        return `${year}-${month}-${day} ${hour}:${minute}:00`;
      }

    createTableIfNotExists();

    const query =`
        INSERT INTO weather_data (DateTime, Temperature, SunshineDuration, ShortwaveRadiation, RelativeHumidity, MeanSeaLevelPressure, SoilTemperature, SoilMoisture, WindSpeed, WindDirection)
        VALUES ?
    `;

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
      console.log('Data inserted successfully!');
      closeConnection();
    }
  });
  });
}

// // // Function to insert data into MongoDB
// // async function insertDataMongoDB(data) {
// //   const client = new MongoClient('mongodb://localhost:5666', { useUnifiedTopology: true, password: 123123 });
// //   try {
// //     await client.connect();
// //     const db = client.db('mongo');
// //     const collection = db.collection('weather_data');
// //     await collection.insertMany(data);
// //   } catch (err) {
// //     console.error('Error occurred:', err);
// //   } finally {
// //     client.close();
// //   }
// // }

// // Function to read data from CSV file and return as an array of objects
function readDataFromCSV(csvFile) {
  return new Promise((resolve, reject) => {
    const data = [];
    fs.createReadStream(csvFile)
      .pipe(csv())
      .on('data', (row) => {
        data.push(row);
      })
      .on('end', () => {
        resolve(data);
      })
      .on('error', (error) => {
        reject(error);
      });
  });
}

(async () => {
  const csvFilePath = './Hourly Weather Data in Gallipoli (2008-2021) short.csv';
  const dataToInsert = await readDataFromCSV(csvFilePath);

  // Measure time for InfluxDB
//   const influxdbStartTime = Date.now();
//   await insertDataInfluxDB(dataToInsert);
//   const influxdbEndTime = Date.now();
//   const influxdbTime = influxdbEndTime - influxdbStartTime;

  // Measure time for TimescaleDB
//   const timescaleDBStartTime = Date.now();
//   await insertDataTimescaleDB(dataToInsert);
//   const timescaleDBEndTime = Date.now();
//   const timescaleDBTime = timescaleDBEndTime - timescaleDBStartTime;

  // Measure time for PostgreSQL
//   const postgresqlStartTime = Date.now();
//   await insertDataPostgreSQL(dataToInsert);
//   const postgresqlEndTime = Date.now();
//   const postgresqlTime = postgresqlEndTime - postgresqlStartTime;

  // Measure time for MySQL
  const mysqlStartTime = Date.now();
  await insertDataMySQL(dataToInsert);
  const mysqlEndTime = Date.now();
  const mysqlTime = mysqlEndTime - mysqlStartTime;

//   // Measure time for MongoDB
//   const mongodbStartTime = Date.now();
//   await insertDataMongoDB(dataToInsert);
//   const mongodbEndTime = Date.now();
//   const mongodbTime = mongodbEndTime - mongodbStartTime;

//   console.log(`InfluxDB insertion time: ${influxdbTime} ms`);
//   console.log(`TimescaleDB insertion time: ${timescaleDBTime} ms`);
//   console.log(`PostgreSQL insertion time: ${postgresqlTime} ms`);
  console.log(`MySQL insertion time: ${mysqlTime} ms`);
//   console.log(`MongoDB insertion time: ${mongodbTime} ms`);
})();

//// Test Influx

// const influx = new InfluxDB({
//   host: 'localhost', // Replace with your InfluxDB host
//   port: 5444, // Replace with your InfluxDB port
//   database: 'influxdb', // Replace with your InfluxDB database name
//   username: 'influx', // Replace with your InfluxDB username (if applicable)
//   password: '123123', // Replace with your InfluxDB password (if applicable)
//   socketTimeout: 60000, 
// });

// async function testConnection() {
//     try {
//       // Ping the InfluxDB server to check the connection
//       const pong = await influx.ping(2000);
//       if (pong) {
//         console.log('Connected to InfluxDB!');
//       } else {
//         console.log('Connection to InfluxDB failed.');
//       }
//     } catch (error) {
//       console.error('Error testing connection to InfluxDB:', error);
//     }
//   }
  
//   // Call the function to test the connection
//   testConnection();

// async function insertDataIntoInfluxDB() {
//     try {
//       const dataPoint = {
//         measurement: 'weather_data', // Replace with your measurement name
//         fields: {
//           temperature: 25.5,
//           humidity: 50.2,
//           sunshine_duration: 180, // in minutes
//           // Add more fields here based on your schema
//         },
//         tags: {
//           location: 'New York', // Replace with a relevant location tag
//           // Add more tags here based on your schema
//         },
//         timestamp: new Date(), // Replace with the appropriate timestamp if needed
//       };
  
//       await influx.writePoints([dataPoint]);
//       console.log('Data inserted successfully into InfluxDB!');
//     } catch (error) {
//       console.error('Error inserting data into InfluxDB:', error);
//     }
//   }
  
//   // Call the function to insert data into InfluxDB
//   insertDataIntoInfluxDB();


///// test postgres

// // PostgreSQL connection configuration
// const config = {
//   user: 'postgres',
//   password: '123123', // Replace with your PostgreSQL password
//   host: 'localhost',
//   port: 5432,
//   database: 'postgres',
// };

// // Function to test the PostgreSQL connection
// async function testPostgresConnection() {
//   const client = new Client(config);

//   try {
//     await client.connect();
//     console.log('Connected to PostgreSQL!');

//     // You can run SQL queries here to interact with the database
//     const res = await client.query('SELECT NOW() as current_time');
//     console.log('Current Time in PostgreSQL:', res.rows[0].current_time);
//   } catch (error) {
//     console.error('Error connecting to PostgreSQL:', error);
//   } finally {
//     await client.end();
//   }
// }

// // Call the function to test the PostgreSQL connection
// testPostgresConnection();