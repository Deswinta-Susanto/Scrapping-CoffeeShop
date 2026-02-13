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

    cityParts.push(arg);
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
  const { city, limit: MAX_DATA, headless } = parseArgs(process.argv.slice(2));
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
  console.log(`Target rows: ${MAX_DATA}`);
  await page.waitForSelector('div[role="article"]', { timeout: 60000 });

  console.log("Start scraping...");
  const schema = new parquet.ParquetSchema({
    name: { type: "UTF8", optional: true },
    address: { type: "UTF8", optional: true },
    phone: { type: "UTF8", optional: true },
    rating: { type: "UTF8", optional: true },
    total_reviews: { type: "UTF8", optional: true },
    cover_image: { type: "UTF8", optional: true },
    gallery_images: { type: "UTF8", optional: true },
    place_url: { type: "UTF8", optional: true },
    reviews: { type: "UTF8", optional: true },
    lat: { type: "DOUBLE", optional: true },
    lng: { type: "DOUBLE", optional: true },
  });

  const outputPath = path.join(__dirname, `maps_${fileSlug}.parquet`);
  const writer = await parquet.ParquetWriter.openFile(schema, outputPath);

  let results = [];
  let index = 0;
  let noNewRounds = 0;
  const seen = new Set();

  while (results.length < MAX_DATA && noNewRounds < 8) {
    const cards = await page.$$('div[role="article"]');

    if (index >= cards.length) {
      console.log("No more visible cards. Scroll to load more...");
      await page.mouse.wheel(0, 6000);
      await page.waitForTimeout(2500);

      const newCount = await page.$$eval(
        'div[role="article"]',
        (els) => els.length
      );

      if (newCount <= cards.length) noNewRounds++;
      else noNewRounds = 0;

      continue;
    }

    const card = cards[index++];
    await card.scrollIntoViewIfNeeded();

    try {
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

    await page.waitForTimeout(3500);
    const data = await page.evaluate(() => {
      const cleanText = (v) => (v ? v.replace(/\s+/g, " ").trim() : null);

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
      let cover_image = null;

      // IMAGE
      const coverImg = document.querySelector(
        'button img[src*="lh3.googleusercontent.com"]'
      );

      if (coverImg) {
        cover_image = coverImg.src;
      }

      let gallery = [];

      const imgs = Array.from(document.querySelectorAll("img"))
        .map((img) => img.src)
        .filter(
          (src) =>
            src &&
            src.includes("lh3.googleusercontent.com") &&
            !src.includes("staticmap") &&
            !src.includes("maps")
        );

      imgs.slice(0, 3).forEach((src) => gallery.push(src));
      
      let reviews = [];
      const reviewEls = document.querySelectorAll("div.MyEned");

      reviewEls.forEach((r, i) => {
        if (i < 3) reviews.push(cleanText(r.innerText));
      });

      // URL + LAT LNG
      const place_url = window.location.href;
      const match = place_url.match(/@(-?\d+\.\d+),(-?\d+\.\d+)/);

      return {
        name,
        address,
        phone,
        rating,
        total_reviews,
        cover_image,
        gallery_images: JSON.stringify(gallery),
        place_url,
        reviews: JSON.stringify(reviews),
        lat: match ? parseFloat(match[1]) : null,
        lng: match ? parseFloat(match[2]) : null,
      };
    });

    data.cover_image = normalizeImageUrl(data.cover_image);

    try {
      const g = JSON.parse(data.gallery_images);
      data.gallery_images = JSON.stringify(
        g.map((x) => normalizeImageUrl(x))
      );
    } catch {}

    if (!data.name || !data.address) continue;

    const key = `${data.name}|${data.address}`.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);

    // LOG
    console.log(`${results.length + 1}. ${data.name}`);
    console.log("   Cover:", data.cover_image);

    results.push(data);
    await writer.appendRow(data);

    await page.waitForTimeout(1200);
  }

  await writer.close();
  await browser.close();

  console.log("\ddone!");
  console.log("Saved:", results.length);
  console.log("save on:", outputPath);
})();
