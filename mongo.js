const fs = require('fs');
const { MongoClient } = require('mongodb');
const csvParser = require('csv-parser');

const mongoURL = 'mongodb://localhost:27017'; // Replace with your MongoDB URL
const dbName = 'mongo'; // Replace with your MongoDB database name
const collectionName = 'weather_data'; // Replace with your collection name
const csvFilePath = './Hourly Weather Data in Gallipoli (2008-2021).csv'; // Replace with the path to your CSV file

const client = new MongoClient(mongoURL);

async function connectAndInsertData() {
    try {
        await client.connect();
        console.log('Connection to MongoDB successful!');

        const db = client.db(dbName);
        const collection = db.collection(collectionName);

        const deleteResult = await collection.deleteMany({});
        console.log(`Deleted ${deleteResult.deletedCount} documents from the collection.`);

        const data = [];

        fs.createReadStream(csvFilePath)
            .pipe(csvParser({ separator: ';' }))
            .on('data', (row) => {
                data.push(row);
            })
            .on('end', async () => {
                try {
                    const startTime = Date.now();

                    const documents = data.map((row) => ({
                        DateTime: convertToDateTime(row['DateTime']),
                        Temperature: parseFloat(row['Temperature']),
                        SunshineDuration: parseFloat(row['SunshineDuration']),
                        ShortwaveRadiation: parseFloat(row['ShortwaveRadiation']),
                        RelativeHumidity: parseFloat(row['RelativeHumidity']),
                        MeanSeaLevelPressure: parseFloat(row['MeanSeaLevelPressure']),
                        SoilTemperature: parseFloat(row['SoilTemperature']),
                        SoilMoisture: parseFloat(row['SoilMoisture']),
                        WindSpeed: parseFloat(row['WindSpeed']),
                        WindDirection: parseFloat(row['WindDirection']),
                    }));

                    const insertManyResult = await collection.insertMany(documents);

                    const endTime = Date.now();
                    const timeTaken = endTime - startTime;
                    console.log(`Data inserted successfully! Time taken: ${timeTaken.toFixed(2)} milliseconds`);
                } catch (err) {
                    console.error('Error inserting data into MongoDB:', err);
                } finally {
                    client.close();
                }
            });
    } catch (err) {
        console.error('Error connecting to MongoDB:', err);
        client.close();
    }
}

function convertToDateTime(dateTimeString) {
    // Assuming the datetime format in the CSV is "DD.MM.YYYY HH:mm"
    const [date, time] = dateTimeString.split(' ');
    const [day, month, year] = date.split('.');
    const [hour, minute] = time.split(':');

    return new Date(`${year}-${month}-${day}T${hour}:${minute}:00Z`);
}

// async function main() {
//   await connectAndInsertData();
// }

// main();

async function getAverageTemperature() {
    const startTime = performance.now();

    try {
        const client = await MongoClient.connect(mongoURL, { useUnifiedTopology: true });
        const db = client.db(dbName);
        const collection = db.collection(collectionName);

        //   const result = await collection.aggregate([
        //     {
        //       $project: {
        //         month_year: { $dateTrunc: { date: '$DateTime', unit: 'month' } },
        //         month: { $month: '$DateTime' },
        //         year: { $year: '$DateTime' },
        //         Temperature: 1,
        //         RelativeHumidity: 1,
        //       },
        //     },
        //     {
        //       $group: {
        //         _id: {
        //           month_year: '$month_year',
        //           month: '$month',
        //           year: '$year',
        //         },
        //         average_temperature: { $avg: '$Temperature' },
        //         average_relative_humidity: { $avg: '$RelativeHumidity' },
        //       },
        //     },
        //     {
        //       $project: {
        //         _id: 0,
        //         month_year: '$_id.month_year',
        //         month: '$_id.month',
        //         year: '$_id.year',
        //         average_temperature: 1,
        //         average_relative_humidity: 1,
        //       },
        //     },
        //     {
        //       $sort: { month_year: 1 },
        //     },
        //   ]).toArray();
        // const result = await collection.aggregate([
        //     {
        //       $group: {
        //         _id: { $dateToString: { format: "%Y-%m-%d", date: "$DateTime" } },
        //         total_records: { $sum: 1 },
        //         average_wind_speed: { $avg: "$WindSpeed" },
        //         max_wind_speed: { $max: "$WindSpeed" },
        //         min_wind_speed: { $min: "$WindSpeed" },
        //         wind_speed_standard_deviation: { $stdDevSamp: "$WindSpeed" },
        //         average_wind_direction: { $avg: "$WindDirection" }
        //       }
        //     },
        //     {
        //       $sort: { total_records: -1 }
        //     },
        //     {
        //       $limit: 5
        //     }
        //   ]).toArray();
        const result = await collection.aggregate([
            {
                $match: {
                    DateTime: {
                        $gte: new Date('2023-01-01'),
                        $lte: new Date('2023-12-31')
                    }
                }
            },
            {
                $group: {
                    _id: {
                        year: { $year: '$DateTime' },
                        month: { $month: '$DateTime' }
                    },
                    average_temperature: { $avg: '$Temperature' },
                    max_temperature: { $max: '$Temperature' },
                    min_temperature: { $min: '$Temperature' },
                    average_relative_humidity: { $avg: '$RelativeHumidity' },
                    max_relative_humidity: { $max: '$RelativeHumidity' },
                    min_relative_humidity: { $min: '$RelativeHumidity' },
                    average_wind_speed: { $avg: '$WindSpeed' },
                    max_wind_speed: { $max: '$WindSpeed' },
                    min_wind_speed: { $min: '$WindSpeed' }
                }
            },
            {
                $sort: { '_id.year': 1, '_id.month': 1 }
            }
        ]).toArray();

        const endTime = performance.now();
        const timeTaken = endTime - startTime;
        console.log(`Time taken: ${timeTaken.toFixed(2)} milliseconds`);

        client.close();
    } catch (err) {
        console.error('Error executing query:', err);
    }
}

getAverageTemperature();
