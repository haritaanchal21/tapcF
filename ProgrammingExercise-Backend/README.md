/*
No of rows in Tables:
- `zz__yashi_cgn`: 9;
- `zz__yashi_cgn_data`: 153;
- `zz__yashi_creative`: 28;
- `zz__yashi_creative_data`: 507;
- `zz__yashi_order`: 16;
- `zz__yashi_order_data`: 333;

Same Sum in all tables:
- SUM(impression_count): 239239
- SUM(click_count): 563
- SUM(25viewed_count): 182431
- SUM(50viewed_count): 171313
- SUM(75viewed_count): 159537
- SUM(100viewed_count): 145742
*/

// Code for reading the csv and then inserting data to different tables
// Before Running the code, please configure your sql from line no 33 - 38

```
const PromiseFtp = require('promise-ftp');
const csv = require('csv-parser');
const mysql = require('mysql');
const moment = require('moment');
const fs = require('fs');
// Read the FTP credentials from file
const credentials = fs.readFileSync('ftp_credentials.txt', 'utf8').trim().split('\n');
const [link, username, password] = credentials;

// Set up the database connection
const connection = mysql.createConnection({
  host: 'localhost',
  user: 'root',
  password: 'root',
  database: 'tapC',
});

// Connect to the database
connection.connect((err) => {
  if (err) throw err;
  console.log('Connected to database!');
});



const ftpCredentials = {
  host: 'ftp.tapclicks.com',
  user: username,
  password: password,
  secure: false
};
const mappings = {};

async function downloadFileFromFtp() {
  const ftp = new PromiseFtp();

  try {
    await ftp.connect(ftpCredentials);
    const fileData = await ftp.get('/data_files/Yashi_Advertisers.csv');
    const localFileStream = fs.createWriteStream('Yashi_Advertisers.csv');
    fileData.pipe(localFileStream);

    console.log('File downloaded successfully');

    const csvStream = csv();

    await new Promise((resolve, reject) => {
      localFileStream.on('finish', () => {
        const readStream = fs.createReadStream('Yashi_Advertisers.csv');
        readStream
          .on('error', (error) => {
            reject(error);
          })
          .pipe(csvStream)
          .on('data', (data) => {
            mappings[data["Advertiser Name"]] = data["Advertiser ID"];
            console.log(mappings[data["Advertiser Name"]]);
          })
          .on('end', () => {
            console.log('Advertiser mappings downloaded!');
            resolve();
          })
          .on('error', (error) => {
            reject(error);
          });
      });
    });
  } catch (err) {
    console.log(err);
  }
}
async function downloadDataFiles() {
  const ftp = new PromiseFtp();
  try {
    await ftp.connect(ftpCredentials);
    const files = await ftp.list(/data_files/);
    for (let file of files) {
      if (file.name.startsWith('Yashi_2016-05')) {
        console.log('Downloading ' + file.name);
        const fileData = await ftp.get(`/data_files/${file.name}`);
        const localFileStream = fs.createWriteStream(file.name);
        fileData.pipe(localFileStream);
        console.log('File downloaded successfully');
        const csvStream = csv();

        await new Promise((resolve, reject) => {
          localFileStream.on('finish', () => {
            const readStream = fs.createReadStream(file.name);
            readStream
              .on('error', (error) => {
                reject(error);
              })
              .pipe(csvStream)
              .on('data', (row) => {
                const advertiserId = row['Advertiser ID'];
                const advertiserName = row['Advertiser Name']
                if (mappings[advertiserName] === advertiserId) {
                  const campaignId = row['Campaign ID'];
                  const orderId = row['Order ID'];
                  const creativeId = row['Creative ID'];
                  const dateStr = row['Date'];
                  const date = Math.floor(new Date(dateStr).getTime() / 1000);
                  const impressionCount = parseInt(row['Impressions']);
                  const clickCount = parseInt(row['Clicks']);
                  const viewedCount25 = parseInt(row['25% Viewed']);
                  const viewedCount50 = parseInt(row['50% Viewed']);
                  const viewedCount75 = parseInt(row['75% Viewed']);
                  const viewedCount100 = parseInt(row['100% Viewed']);

                  const name = row['Campaign Name']

                  // Insert campaign data
                  connection.query(
                    'INSERT INTO zz__yashi_cgn (campaign_id, name,yashi_advertiser_id,advertiser_name) VALUES (?, ?, ?, ?) ON DUPLICATE KEY UPDATE name = ?',
                    [campaignId, name, advertiserId, advertiserName, row['Campaign Name']],
                    function (error, results, fields) {
                      if (error) {
                        console.error(error);
                      }
                    }
                  );

                  // Insert campaign data per day
                  connection.query(
                    'INSERT INTO zz__yashi_cgn_data (campaign_id, log_date, impression_count, click_count, 25viewed_count, 50viewed_count, 75viewed_count, 100viewed_count) VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE impression_count = impression_count + ?, click_count = click_count + ?, 25viewed_count = 25viewed_count + ?, 50viewed_count = 50viewed_count + ?, 75viewed_count = 75viewed_count + ?, 100viewed_count = 100viewed_count + ?',
                    [campaignId, date, impressionCount, clickCount, viewedCount25, viewedCount50, viewedCount75, viewedCount100, impressionCount, clickCount, viewedCount25, viewedCount50, viewedCount75, viewedCount100],
                    function (error, results, fields) {
                      if (error) {
                        console.error(error);
                      }
                    }
                  );

                  // Insert order data
                  connection.query(
                    'INSERT INTO zz__yashi_order (order_id, campaign_id, name) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE name = ?',
                    [orderId, campaignId, row['Order Name'], row['Order Name']],
                    function (error, results, fields) {
                      if (error) {
                        console.error(error);
                      }
                    }
                  );

                  // Insert order data
                  connection.query(
                    'INSERT INTO zz__yashi_order_data (order_id, log_date, impression_count, click_count, 25viewed_count, 50viewed_count, 75viewed_count, 100viewed_count) VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE impression_count = impression_count + ?, click_count = click_count + ?, 25viewed_count = 25viewed_count + ?, 50viewed_count = 50viewed_count + ?, 75viewed_count = 75viewed_count + ?, 100viewed_count = 100viewed_count + ?',
                    [orderId, date, impressionCount, clickCount, viewedCount25, viewedCount50, viewedCount75, viewedCount100, impressionCount, clickCount, viewedCount25, viewedCount50, viewedCount75, viewedCount100],
                    function (error, results, fields) {
                      if (error) {
                        console.error(error);
                      }
                    }
                  );
                  // Insert creative data
                  connection.query(
                    'INSERT INTO zz__yashi_creative (creative_id,order_id,name,preview_url) VALUES (?, ?, ?,?) ON DUPLICATE KEY UPDATE name = ?',
                    [creativeId, orderId, row['Creative Name'], row['Creative Preview URL'], row['Creative Name']],
                    function (error, results, fields) {
                      if (error) {
                        console.error(error);
                      }
                    }
                  );
                  // Insert creative data per day
                  connection.query(
                    'INSERT INTO zz__yashi_creative_data (creative_id, log_date, impression_count, click_count, 25viewed_count, 50viewed_count, 75viewed_count, 100viewed_count) VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE impression_count = impression_count + ?, click_count = click_count + ?, 25viewed_count = 25viewed_count + ?, 50viewed_count = 50viewed_count + ?, 75viewed_count = 75viewed_count + ?, 100viewed_count = 100viewed_count + ?',
                    [creativeId, date, impressionCount, clickCount, viewedCount25, viewedCount50, viewedCount75, viewedCount100, impressionCount, clickCount, viewedCount25, viewedCount50, viewedCount75, viewedCount100],
                    function (error, results, fields) {
                      if (error) {
                        console.error(error);
                      }
                    }
                  );

                }
              })
              .on('end', () => {
                console.log('Advertiser mappings downloaded!');
                resolve();
              })
              .on('error', (error) => {
                reject(error);
              });
          });
        });
      }
    }
  } catch (err) {
    console.error(err);
  } finally {
  }
}

async function main() {
  await downloadFileFromFtp();
  await downloadDataFiles();
}
main();
```
