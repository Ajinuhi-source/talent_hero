// due to puppeteer-extra-plugin-stealth, we can not change default folder for downloads
// due to puppeteer-extra-plugin-stealth, we can not wait for page to load, we have to use timers

const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
const login = require('./login');
puppeteer.use(StealthPlugin());

const fs = require('fs');
const path = require('path');

const downloadPath = '/Users/mike/Downloads';



// take email, password, url as arguments from command line

const email = process.argv[2];
const password = process.argv[3];
const rank_tracker_url = process.argv[4];        

async function goToRankTracker(page) {
  await page.goto('https://app.ahrefs.com/rank-tracker');
  console.log('Went to rank tracker page');
}

async function goToOverviewPage(page, rank_tracker_url) {
  await page.waitForTimeout(5000);

  await page.goto(rank_tracker_url);
  console.log('Opened overview page');

  await page.waitForTimeout(2000);

  // extract text from css selector ".ProjectDropdown-targetToggle"
  const project_name = await page.evaluate(() => document.querySelector('.ProjectDropdown-targetToggle').textContent);
  console.log('Project name:', project_name);
  await page.waitForTimeout(2000);

  return project_name;

}

async function scrollPage(page) {
  await page.mouse.wheel({ deltaY: 200 });
  await page.waitForTimeout(2000);
}

async function clickExportButton(page) {
  await page.click('.Export');
  console.log('Clicked on first export button');
  await page.waitForTimeout(4000);
}

async function clickSecondExportButton(page) {
  await page.click('.btn.btn--primary');
  console.log('Clicked on second export button');
  await page.waitForTimeout(4000);
}

async function checkDownloadStatus(page) {
  await page.waitForSelector('.Export__status.Export__status--success', { timeout: 0 });
  console.log('Download successfully finished');
  // go to downloads folder and check if file is there

}

function getLatestFile() {
  // Get list of files in download directory
  const files = fs.readdirSync(downloadPath);

  // Filter out files that start with a period (.) or underscore (_)
  const filteredFiles = files.filter(file => !file.startsWith('.') && !file.startsWith('_'));

  // Sort files by modified time (most recent first)
  const sortedFiles = filteredFiles.sort((a, b) => {
    return fs.statSync(downloadPath + '/' + b).mtime.getTime() -
           fs.statSync(downloadPath + '/' + a).mtime.getTime();
  });

  // Return the most recent file path
  return sortedFiles[0]; 
}


function renameAndMoveLatestFile(project_name, latestFilePath, newDir) {
  const latestFileName = path.basename(latestFilePath);
  const newName = `${project_name}.csv`;
  const newPath = path.join(newDir, newName);

  // Rename the file
  fs.rename(latestFilePath, newPath, (err) => {
    if (err) throw err;
    console.log('File renamed successfully');

    // Move the file to the new directory
    fs.rename(newPath, path.join(newDir, '..', newName), (err) => {
      if (err) throw err;
      console.log('File moved successfully');
    });
  });
}


(async () => {
  const browser = await puppeteer.launch({ headless: false });
  const page = await browser.newPage();

  await login(page, email, password);
  await goToRankTracker(page);
  const project_name = await goToOverviewPage(page, rank_tracker_url);
  await scrollPage(page);
  await clickExportButton(page);
  await clickSecondExportButton(page);
  // await checkDownloadStatus(page);
  await page.waitForTimeout(2000);
  await browser.close();  
  console.log('Browser closed');
  // Go to download folder
  const latestFile = getLatestFile();

  fs.rename(downloadPath+"/"+latestFile, project_name + ".csv", (err) => {
    if (err) throw err;
    console.log('File renamed successfully');
    console.log(downloadPath+"/"+latestFile);

  });

})();

