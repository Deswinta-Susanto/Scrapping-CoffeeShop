const { chromium } = require("playwright");
const parquet = require("parquetjs-lite");
const path = require("path");

(async () => {
  const browser = await chromium.launch({
    headless: false,
    slowMo: 50,
  });

  const page = await browser.newPage({
    viewport: { width: 1280, height: 800 },
  });

  await page.goto(
    "https://www.google.com/maps/search/coffee+shop+in+colomadu",
    { waitUntil: "domcontentloaded", timeout: 60000 }
  );

  console.log("pages loaded");
  await page.waitForSelector('div[role="article"]', { timeout: 60000 });
  let prevCount = 0;
  let sameCount = 0;

  while (sameCount < 3) {
    await page.mouse.wheel(0, 6000);
    await page.waitForTimeout(2000);

    const currentCount = await page.$$eval(
      'div[role="article"]',
      (els) => els.length
    );

    console.log("Cards loaded:", currentCount);

    if (currentCount === prevCount) sameCount++;
    else {
      sameCount = 0;
      prevCount = currentCount;
    }
  }

  console.log("Start scraping...");
  const schema = new parquet.ParquetSchema({
    name: { type: "UTF8", optional: true },
    address: { type: "UTF8", optional: true },
    phone: { type: "UTF8", optional: true },
    rating: { type: "UTF8", optional: true },
    total_reviews: { type: "UTF8", optional: true },
    image: { type: "UTF8", optional: true },
    reviews: { type: "UTF8", optional: true },
    lat: { type: "DOUBLE", optional: true },
    lng: { type: "DOUBLE", optional: true },
  });

  const outputPath = path.join(__dirname, "maps_full.parquet");
  const writer = await parquet.ParquetWriter.openFile(schema, outputPath);

  const MAX_DATA = 20;
  let results = [];
  let index = 0;

  while (results.length < MAX_DATA) {
    const cards = await page.$$('div[role="article"]');

    if (index >= cards.length) {
      console.log("Scroll again");
      await page.mouse.wheel(0, 6000);
      await page.waitForTimeout(2000);
      continue;
    }

    const card = cards[index];

    try {
      await card.scrollIntoViewIfNeeded();
      await card.click();
    } catch {
      index++;
      continue;
    }

    await page.waitForSelector("h1.DUwDvf", { timeout: 15000 });
    await page.waitForTimeout(1000);
    const data = await page.evaluate(() => {
      // NAME
      const name =
        document.querySelector("h1.DUwDvf")?.innerText.trim() || null;

      // ADDRESS
      const address =
        document.querySelector('button[data-item-id="address"]')
          ?.innerText.trim() || null;

      // PHONE
      const phone =
        document.querySelector('button[data-item-id^="phone"]')
          ?.innerText.trim() || null;

      // RATING + TOTAL REVIEW
      let rating = null;
      let total_reviews = null;

      const ratingBlock = document.querySelector("div.F7nice");

      if (ratingBlock) {
        rating =
          ratingBlock.querySelector("span")?.innerText.trim() || null;

        total_reviews =
          ratingBlock.querySelector("span:nth-child(2)")
            ?.innerText.replace(/[()]/g, "")
            .trim() || null;
      }

      // IMAGE
      const image =
        document.querySelector("button img")?.src || null;

      // LAT LNG
      const match = window.location.href.match(
        /@(-?\d+\.\d+),(-?\d+\.\d+)/
      );

      // REVIEWS (3)
      let reviews = [];
      const reviewEls = document.querySelectorAll(
        'div[data-review-id]'
      );

      reviewEls.forEach((r, i) => {
        if (i < 3) {
          reviews.push(r.innerText.trim());
        }
      });

      return {
        name,
        address,
        phone,
        rating,
        total_reviews,
        image,
        reviews: JSON.stringify(reviews),
        lat: match ? parseFloat(match[1]) : null,
        lng: match ? parseFloat(match[2]) : null,
      };
    });

    console.log(`${results.length + 1}. ${data.name}`);
    console.log("   Rating:", data.rating);
    console.log("   Image:", data.image);

    results.push(data);
    await writer.appendRow(data);

    index++;
    await page.waitForTimeout(1500);
  }

  await writer.close();
  await browser.close();

  console.log("\ndone!");
  console.log("save on:", outputPath);
})();
