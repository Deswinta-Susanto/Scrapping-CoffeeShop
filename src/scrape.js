const { chromium } = require("playwright");
const parquet = require("parquetjs-lite");
const path = require("path");

function parseArgs(argv) {
  let limit = 150;
  let headless = false;
  const cityParts = [];

  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];

    if (arg === "--headless") {
      headless = true;
      continue;
    }

    if (arg === "--limit" && argv[i + 1]) {
      limit = Number(argv[i + 1]);
      i++;
      continue;
    }

    if (arg.startsWith("--limit=")) {
      limit = Number(arg.split("=")[1]);
      continue;
    }

    cityParts.push(arg);
  }

  if (!Number.isFinite(limit) || limit <= 0) {
    limit = 150;
  }

  return {
    city: cityParts.join(" ").trim() || "surakarta",
    limit,
    headless,
  };
}

function toSlug(text) {
  return (
    text
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "_")
      .replace(/^_+|_+$/g, "") || "results"
  );
}

function normalizeImageUrl(url) {
  if (!url) return null;

  return url
    .replace(/=w\d+-h\d+(-k-no)?(-p)?/gi, "=s0")
    .replace(/=s\d+(-k-no)?(-p)?/gi, "=s0");
}

(async () => {
  const { city, limit: maxData, headless } = parseArgs(process.argv.slice(2));
  const searchQuery = `coffee shop in ${city}`;
  const searchUrl = `https://www.google.com/maps/search/${encodeURIComponent(
    searchQuery
  )}`;
  const fileSlug = toSlug(city);

  const browser = await chromium.launch({
    headless,
    slowMo: headless ? 0 : 50,
  });

  const page = await browser.newPage({
    viewport: { width: 1280, height: 800 },
  });

  await page.goto(
    searchUrl,
    { waitUntil: "domcontentloaded", timeout: 60000 }
  );

  console.log(`Pages loaded for query: "${searchQuery}"`);
  console.log(`Target rows: ${maxData}`);
  await page.waitForSelector('div[role="article"]', { timeout: 60000 });

  console.log("Start scraping...");
  const schema = new parquet.ParquetSchema({
    name: { type: "UTF8", optional: true },
    address: { type: "UTF8", optional: true },
    phone: { type: "UTF8", optional: true },
    rating: { type: "UTF8", optional: true },
    total_reviews: { type: "UTF8", optional: true },
    image: { type: "UTF8", optional: true },
    place_url: { type: "UTF8", optional: true },
    reviews: { type: "UTF8", optional: true },
    lat: { type: "DOUBLE", optional: true },
    lng: { type: "DOUBLE", optional: true },
  });

  const outputPath = path.join(__dirname, `maps_${fileSlug}.parquet`);
  const writer = await parquet.ParquetWriter.openFile(schema, outputPath);

  const MAX_DATA = maxData;
  const MAX_NO_NEW_CARD_ROUNDS = 8;
  const seen = new Set();
  let duplicateCount = 0;
  let noNewCardRounds = 0;
  let lastCardCount = 0;
  let results = [];
  let index = 0;

  while (results.length < MAX_DATA && noNewCardRounds < MAX_NO_NEW_CARD_ROUNDS) {
    const cards = await page.$$('div[role="article"]');

    if (index >= cards.length) {
      console.log("No more visible cards. Scroll to load more...");
      await page.mouse.wheel(0, 6000);
      await page.waitForTimeout(2500);

      const afterScrollCount = await page.$$eval(
        'div[role="article"]',
        (els) => els.length
      );

      if (afterScrollCount <= cards.length) {
        noNewCardRounds++;
        console.log(
          `No new cards loaded (${noNewCardRounds}/${MAX_NO_NEW_CARD_ROUNDS})`
        );
      } else {
        noNewCardRounds = 0;
        lastCardCount = afterScrollCount;
      }

      continue;
    }

    const card = cards[index];
    index++;

    try {
      await card.scrollIntoViewIfNeeded();
      await card.click();
    } catch (err) {
      console.log("Skip card click error:", err.message);
      continue;
    }

    try {
      await page.waitForSelector("h1.DUwDvf", { timeout: 15000 });
    } catch {
      continue;
    }

    await page.waitForTimeout(1000);
    const data = await page.evaluate(() => {
      const cleanText = (value) => {
        if (!value) return null;
        const cleaned = value
          .replace(/^[^\p{L}\p{N}]+/gu, "")
          .replace(/\s+/g, " ")
          .trim();
        return cleaned || null;
      };

      // NAME
      const name = cleanText(document.querySelector("h1.DUwDvf")?.innerText);

      // ADDRESS
      const address = cleanText(
        document.querySelector('button[data-item-id="address"]')?.innerText
      );

      // PHONE
      const phone = cleanText(
        document.querySelector('button[data-item-id^="phone"]')?.innerText
      );

      // RATING + TOTAL REVIEW
      let rating = null;
      let total_reviews = null;

      const ratingBlock = document.querySelector("div.F7nice");

      if (ratingBlock) {
        rating = cleanText(ratingBlock.querySelector("span")?.innerText);

        total_reviews =
          ratingBlock.querySelector("span:nth-child(2)")
            ?.innerText.replace(/[()]/g, "")
            .trim() || null;
      }

      // IMAGE
      const galleryImage = Array.from(document.querySelectorAll("img"))
        .map((img) => img.src)
        .find((src) => src && src.includes("googleusercontent.com/p/"));

      const image =
        galleryImage ||
        document.querySelector("button img")?.src ||
        document.querySelector('meta[property="og:image"]')?.content ||
        null;

      const place_url = window.location.href;

      // LAT LNG
      const match = place_url.match(
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
        place_url,
        reviews: JSON.stringify(reviews),
        lat: match ? parseFloat(match[1]) : null,
        lng: match ? parseFloat(match[2]) : null,
      };
    });

    data.image = normalizeImageUrl(data.image);

    const dedupeKey = [data.name, data.address]
      .map((v) => (v || "").toLowerCase().trim())
      .join("|");

    if (!data.name || !data.address) {
      console.log("Skip incomplete data");
      continue;
    }

    if (seen.has(dedupeKey)) {
      duplicateCount++;
      console.log(`Duplicate skipped: ${data.name}`);
      continue;
    }

    seen.add(dedupeKey);

    console.log(`${results.length + 1}. ${data.name}`);
    console.log("   Rating:", data.rating);
    console.log("   Image:", data.image);

    results.push(data);
    await writer.appendRow(data);

    if (cards.length > lastCardCount) {
      lastCardCount = cards.length;
    }

    await page.waitForTimeout(1500);
  }

  await writer.close();
  await browser.close();

  console.log("\ndone!");
  console.log(`Saved rows: ${results.length}`);
  console.log(`Duplicates skipped: ${duplicateCount}`);
  console.log(`Unique cards seen: ${seen.size}`);
  console.log("save on:", outputPath);
})();
