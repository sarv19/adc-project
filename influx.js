const fs = require('fs');
const axios = require('axios');
const { InfluxDB, Point, flux } = require('@influxdata/influxdb-client');
const csvParser = require('csv-parser');

const influxURL = 'http://localhost:8086'; // Replace with your InfluxDB URL
const influxToken = 'MyLFUR6JIW9EvbnZ3v_xAEctweSV1zGji25VtTBdx4Il1-5VdZpXloYPICf9-NVhyFU9Y_eDGLxv1jhhzVjwRQ=='; // Replace with your InfluxDB token
const influxOrg = 'adc'; // Replace with your InfluxDB organization
const influxBucket = 'influxBucket'; // Replace with your InfluxDB bucket
const csvFilePath = './Hourly Weather Data in Gallipoli (2008-2021).csv'; // Replace with the path to your CSV file
const measurementName = 'weather_data';

const influxDB = new InfluxDB({ url: influxURL, token: influxToken });

async function createBucket() {
    const writeApi = influxDB.getWriteApi(influxOrg, influxBucket, 's');
    const bucketName = influxBucket;
    const retentionRules = [
        { type: 'expire', everySeconds: 3600 * 24 * 30 }, // Set your desired retention policy (e.g., 30 days)
    ];

    try {
        const point = new Point('dummy_measurement').floatField('dummy_field', 0);
        writeApi.writePoint(point);
        await writeApi.flush();
        console.log(`Bucket "${influxBucket}" created.`);
    } catch (err) {
        // Ignore the error if the bucket already exists
        if (!err.message.includes('duplicate')) {
            console.error('Error creating bucket:', err);
        }
    }
}

async function insertDataIntoInfluxDB() {
    const data = [];

    fs.createReadStream(csvFilePath)
        .pipe(csvParser({ separator: ';' }))
        .on('data', (row) => {
            data.push(row);
        })
        .on('end', async () => {
            const writeApi = influxDB.getWriteApi(influxOrg, influxBucket, 'ms');

            const deleteURL = `${influxURL}/api/v2/delete?org=your_org&bucket=${influxBucket}&predicate=_measurement="weather_data"`;
            const headers = { Authorization: 'Token your_influxdb_token' }; // Replace with your InfluxDB token
            const response = influxDB.getWriteApi(deleteURL, { headers });

            const startTime = performance.now();

            for (const row of data) {
                const dateTime = convertToTimestamp(row['DateTime']);

                const point = new Point('weather_data')
                    .stringField('DateTime', dateTime)
                    .floatField('Temperature', parseFloat(row['Temperature']))
                    .floatField('SunshineDuration', parseFloat(row['SunshineDuration']))
                    .floatField('ShortwaveRadiation', parseFloat(row['ShortwaveRadiation']))
                    .floatField('RelativeHumidity', parseFloat(row['RelativeHumidity']))
                    .floatField('MeanSeaLevelPressure', parseFloat(row['MeanSeaLevelPressure']))
                    .floatField('SoilTemperature', parseFloat(row['SoilTemperature']))
                    .floatField('SoilMoisture', parseFloat(row['SoilMoisture']))
                    .floatField('WindSpeed', parseFloat(row['WindSpeed']))
                    .floatField('WindDirection', parseFloat(row['WindDirection']));

                writeApi.writePoint(point);
            }

            writeApi
                .close()
                .then(() => {
                    const endTime = performance.now();
                    const timeTaken = endTime - startTime;
                    console.log(`Data inserted successfully! Time taken: ${timeTaken.toFixed(2)} milliseconds`);
                })
                .catch((err) => {
                    console.error('Error writing data:', err);
                });
        });
}

function convertToTimestamp(dateTimeString) {
    // Assuming the datetime format in the CSV is "DD.MM.YYYY HH:mm"
    const [date, time] = dateTimeString?.split(' ');
    const [day, month, year] = date?.split('.');
    const [hour, minute] = time?.split(':');

    const timestamp1 = new Date(`${year}-${month}-${day} ${hour}:${minute}:00`);
    return timestamp1;
}

async function executeFluxQuery(query) {
    // const influxURL = 'http://localhost:8086/api/v2/query?org=' + encodeURIComponent(influxOrg);

    try {
        const response = await axios.post(influxURL, {
            query: query,
        }, {
            headers: {
                Authorization: `Token ${influxToken}`,
            },
        });

        return response.data;
    } catch (err) {
        if (err.response && err.response.data && err.response.data.error) {
            const errorMessage = err.response.data.error;
            console.error('Flux query execution error:', errorMessage);
            throw new Error('Error executing Flux query');
        } else {
            console.error('Error executing Flux query:', err.message);
            throw new Error('Error executing Flux query');
        }
    }
}

async function getAverageTemperature() {
    const startTime = performance.now();

    // const query = flux`
    //     from(bucket: ${influxBucket})
    //     |> range(start: 2020-02-01T00:00:00Z, stop: 2020-05-31T23:59:59Z)
    //     |> filter(fn: (r) => r._measurement == "weather_data")
    //     |> aggregateWindow(every: 30d, fn: mean, createEmpty: false)
    //     |> yield(name: "mean_result")
    // `;

    // const query = flux`
    // from(bucket: ${influxBucket})
    //     |> range(start: 2022-01-01T00:00:00Z, stop: 2022-12-31T23:59:59Z)
    //     |> group(columns: ["_time"])
    //     |> count()
    //     |> map(fn: (r) => ({ r with total_records: int(v: r._value) }))
    //     |> keep(columns: ["_time", "total_records"])
    //     |> aggregateWindow(every: 1d, fn: mean, createEmpty: false)
    //     |> yield(name: "avg_wind_speed")
    //     |> map(fn: (r) => ({ r with
    //         average_wind_speed: r._value,
    //         max_wind_speed: r._value,
    //         min_wind_speed: r._value
    //     }))
    //     |> drop(columns: ["_value"])
    //     |> join(tables: {shortwave_radiation: {bucket: ${influxBucket}, keyColumns: ["_time"], valueColumns: ["shortwave_radiation"]}}, on: ["_time"])
    //     |> reduce(fn: (r, accumulator) => ({
    //         max_sunshine_duration: if r.shortwave_radiation > accumulator.max_sunshine_duration then r.shortwave_radiation else accumulator.max_sunshine_duration
    //     }), identity: {max_sunshine_duration: 0.0})
    //     |> yield(name: "max_sunshine_duration")
    //     |> group(columns: ["_time"])
    //     |> aggregateWindow(every: 1d, fn: mean, createEmpty: false)
    //     |> map(fn: (r) => ({ r with wind_speed_standard_deviation: sqrt(sum(column: "_value") / count(column: "_value")) }))
    //     |> yield(name: "wind_speed_standard_deviation")
    //     |> join(tables: {wind_direction: {bucket: ${influxBucket}, keyColumns: ["_time"], valueColumns: ["wind_direction"]}}, on: ["_time"])
    //     |> group(columns: ["_time"])
    //     |> aggregateWindow(every: 1d, fn: mean, createEmpty: false)
    //     |> map(fn: (r) => ({ r with average_wind_direction: r._value }))
    //     |> drop(columns: ["_value"])
    //     |> yield(name: "average_wind_direction")
    //     |> sort(columns: ["total_records"], desc: true)
    //     |> limit(n: 5)
    // `;

    const query = flux`
  from(bucket: ${influxBucket})
    |> range(start: 2023-01-01T00:00:00Z, stop: 2023-12-31T23:59:59Z)
    |> group(columns: ["_time"])
    |> aggregateWindow(every: 1mo, fn: mean, createEmpty: false)
    |> yield(name: "average_temperature", tagColumns: ["_start"])
    |> map(fn: (r) => ({ r with 
        max_temperature: r._value,
        min_temperature: r._value
    }))
    |> join(tables: {humidity: {bucket: ${influxBucket}, keyColumns: ["_time"], valueColumns: ["RelativeHumidity"]}}, on: ["_time"])
    |> group(columns: ["_time"])
    |> aggregateWindow(every: 1mo, fn: mean, createEmpty: false)
    |> yield(name: "average_relative_humidity", tagColumns: ["_start"])
    |> map(fn: (r) => ({ r with 
        max_relative_humidity: r._value,
        min_relative_humidity: r._value
    }))
    |> join(tables: {wind: {bucket: ${influxBucket}, keyColumns: ["_time"], valueColumns: ["WindSpeed"]}}, on: ["_time"])
    |> group(columns: ["_time"])
    |> aggregateWindow(every: 1mo, fn: mean, createEmpty: false)
    |> yield(name: "average_wind_speed", tagColumns: ["_start"])
    |> map(fn: (r) => ({ r with 
        max_wind_speed: r._value,
        min_wind_speed: r._value
    }))
`;

    try {
        const result = await executeFluxQuery(query);

        const endTime = performance.now();
        const timeTaken = endTime - startTime;
        console.log(`Time taken: ${timeTaken.toFixed(2)} milliseconds`);

        // Process the query results here
        const meanResult = result?.results?.[0]?.series?.[0]?.values;
        if (meanResult) {
            meanResult.forEach((row) => {
                const averageTemperature = row[1];
                const averageRelativeHumidity = row[2];
                console.log(`Average Temperature: ${averageTemperature}`);
                console.log(`Average Relative Humidity: ${averageRelativeHumidity}`);
            });
        } else {
            console.log('No results found.');
        }
    } catch (err) {
        console.error('Error executing query:', err);
    }
}

getAverageTemperature();

// async function main() {
//     await createBucket();
//     await insertDataIntoInfluxDB();
// }

// main();
