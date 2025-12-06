#!/usr/bin/env node
/**
 * Visible Connect Full Database Scraper
 * 
 * Scrapes ALL data from https://connect.visible.vc/investors including:
 * - Investor profiles (name, location, check size, stages, focuses, etc.)
 * - Team members (names, roles, LinkedIn profiles)
 * - Recent investments/rounds (company, amount, date, location)
 * 
 * Outputs database-ready JSON and CSV files with normalized tables.
 * 
 * Usage:
 *   node visible_full_scraper.js [options]
 * 
 * Options:
 *   --limit <n>      Maximum number of investors to fetch (default: all ~19,300)
 *   --delay <ms>     Delay between requests in milliseconds (default: 800)
 *   --output <dir>   Output directory name (default: visible_db_export)
 *   --resume         Resume from last saved progress
 */

const https = require('https');
const fs = require('fs');
const path = require('path');

// ============================================================================
// Configuration
// ============================================================================

const CONFIG = {
    API_BASE: 'project-valentine-api.herokuapp.com',
    INVESTORS_ENDPOINT: '/investors',
    PEOPLE_ENDPOINT: '/people',
    ROUNDS_ENDPOINT: '/rounds',
    PAGE_SIZE: 100,
    
    // Conservative rate limiting for overnight runs
    DEFAULT_DELAY: 2000,        // 2 seconds between requests
    MIN_DELAY_BETWEEN_CALLS: 1500, // Minimum 1.5s between any API call
    
    // Never give up - infinite retries with exponential backoff
    MAX_RETRIES: Infinity,      // Never stop retrying
    INITIAL_RETRY_DELAY: 30000, // Start with 30 second retry delay
    MAX_RETRY_DELAY: 300000,    // Max 5 minutes between retries
    
    // Rate limit protection: regular breaks
    PAUSE_EVERY: 50,            // Take a break every 50 investors
    PAUSE_DURATION: 10000,      // 10 second break
    
    // Save progress frequently
    SAVE_EVERY: 10,             // Save progress every 10 investors
    
    OUTPUT_DIR: path.join(__dirname, 'results'),
    USER_AGENT: 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
};

// ============================================================================
// Utility Functions
// ============================================================================

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

function parseArgs() {
    const args = process.argv.slice(2);
    const options = {
        limit: Infinity,
        delay: CONFIG.DEFAULT_DELAY,
        output: 'visible_db_export',
        resume: false,
    };

    for (let i = 0; i < args.length; i++) {
        switch (args[i]) {
            case '--limit': options.limit = parseInt(args[++i], 10); break;
            case '--delay': options.delay = parseInt(args[++i], 10); break;
            case '--output': options.output = args[++i]; break;
            case '--resume': options.resume = true; break;
            case '--help':
            case '-h':
                printHelp();
                process.exit(0);
        }
    }
    return options;
}

function printHelp() {
    console.log(`
╔══════════════════════════════════════════════════════════════════════════════╗
║               Visible Connect Full Database Scraper                          ║
║                     🌙 Designed for overnight runs 🌙                        ║
╚══════════════════════════════════════════════════════════════════════════════╝

Scrapes ALL investor data including team members and investment rounds.
Outputs normalized database tables ready for import.

USAGE:
  node visible_full_scraper.js [options]

OPTIONS:
  --limit <n>      Maximum investors to fetch (default: all ~19,300)
  --delay <ms>     Delay between requests in ms (default: ${CONFIG.DEFAULT_DELAY})
  --output <dir>   Output directory name (default: visible_db_export)
  --resume         Resume from last saved progress
  --help, -h       Show this help

OVERNIGHT-SAFE FEATURES:
  ✓ Never crashes - infinite retries with exponential backoff
  ✓ Checkpoint every ${CONFIG.PAUSE_EVERY} investors (${CONFIG.PAUSE_DURATION/1000}s pause)
  ✓ ${CONFIG.DEFAULT_DELAY/1000}s delay between requests
  ✓ Auto-saves progress every ${CONFIG.SAVE_EVERY} investors
  ✓ Resumes exactly where it left off

OUTPUT FILES:
  investors.json/csv     - Main investor profiles
  team_members.json/csv  - Team members with investor_id foreign key
  investments.json/csv   - Investment rounds with investor_id foreign key
  full_database.json     - Complete nested data structure
  schema.sql             - SQL schema for database import

EXAMPLES:
  # Run overnight (safe, will never crash)
  node visible_full_scraper.js

  # Test with first 50 investors
  node visible_full_scraper.js --limit 50

  # Resume after stopping or restarting
  node visible_full_scraper.js --resume

  # Extra conservative (3 second delays)
  node visible_full_scraper.js --delay 3000
`);
}

// ============================================================================
// HTTP Request Handler
// ============================================================================

// Calculate exponential backoff delay
function getRetryDelay(retryCount) {
    // Exponential backoff: 30s, 60s, 120s, 240s... capped at 5 minutes
    const delay = Math.min(
        CONFIG.INITIAL_RETRY_DELAY * Math.pow(2, retryCount),
        CONFIG.MAX_RETRY_DELAY
    );
    // Add some jitter (±20%) to avoid thundering herd
    const jitter = delay * 0.2 * (Math.random() - 0.5);
    return Math.round(delay + jitter);
}

function makeRequest(endpoint, retryCount = 0) {
    return new Promise((resolve, reject) => {
        const options = {
            hostname: CONFIG.API_BASE,
            path: endpoint,
            method: 'GET',
            headers: {
                'Accept': 'application/vnd.api+json',
                'Content-Type': 'application/vnd.api+json',
                'User-Agent': CONFIG.USER_AGENT,
                'Referer': 'https://connect.visible.vc/investors',
                'Origin': 'https://connect.visible.vc',
            }
        };

        const req = https.request(options, (res) => {
            let data = '';
            res.on('data', chunk => data += chunk);
            res.on('end', () => {
                if (res.statusCode === 200) {
                    try {
                        resolve(JSON.parse(data));
                    } catch (e) {
                        // JSON parse error - retry
                        const delay = getRetryDelay(retryCount);
                        console.log(`\n⚠️  JSON parse error. Retry #${retryCount + 1} in ${Math.round(delay/1000)}s...`);
                        sleep(delay).then(() => resolve(makeRequest(endpoint, retryCount + 1)));
                    }
                } else if (res.statusCode === 429) {
                    // Rate limited - use longer delay
                    const delay = getRetryDelay(retryCount) * 2; // Double the delay for rate limits
                    console.log(`\n⏳ Rate limited (429). Retry #${retryCount + 1} in ${Math.round(delay/1000)}s...`);
                    sleep(delay).then(() => resolve(makeRequest(endpoint, retryCount + 1)));
                } else if (res.statusCode >= 500) {
                    // Server error - retry with backoff
                    const delay = getRetryDelay(retryCount);
                    console.log(`\n⚠️  Server error (${res.statusCode}). Retry #${retryCount + 1} in ${Math.round(delay/1000)}s...`);
                    sleep(delay).then(() => resolve(makeRequest(endpoint, retryCount + 1)));
                } else if (res.statusCode >= 400) {
                    // Client error (4xx except 429) - skip this item but don't crash
                    console.log(`\n⚠️  Client error (${res.statusCode}) for ${endpoint.substring(0, 50)}... - skipping`);
                    resolve(null); // Return null to indicate skip
                } else {
                    // Unknown status - retry
                    const delay = getRetryDelay(retryCount);
                    console.log(`\n⚠️  Unexpected status ${res.statusCode}. Retry #${retryCount + 1} in ${Math.round(delay/1000)}s...`);
                    sleep(delay).then(() => resolve(makeRequest(endpoint, retryCount + 1)));
                }
            });
        });

        req.on('error', (error) => {
            // Network error - always retry with backoff
            const delay = getRetryDelay(retryCount);
            console.log(`\n⚠️  Network error: ${error.message}. Retry #${retryCount + 1} in ${Math.round(delay/1000)}s...`);
            sleep(delay).then(() => resolve(makeRequest(endpoint, retryCount + 1)));
        });

        req.setTimeout(60000, () => { // Increased timeout to 60s
            req.destroy();
            // Timeout - retry with backoff
            const delay = getRetryDelay(retryCount);
            console.log(`\n⚠️  Request timeout. Retry #${retryCount + 1} in ${Math.round(delay/1000)}s...`);
            sleep(delay).then(() => resolve(makeRequest(endpoint, retryCount + 1)));
        });

        req.end();
    });
}

// ============================================================================
// Data Extraction Functions
// ============================================================================

function extractInvestor(data) {
    const attrs = data.attributes || {};
    return {
        id: data.id,
        name: attrs.name || '',
        slug: attrs.slug || '',
        website: (attrs.website || '').trim(),
        secondary_website: (attrs['secondary-website'] || '').trim(),
        description: (attrs.description || '').replace(/[\r\n]+/g, ' ').trim(),
        
        // Location
        city: attrs.city || '',
        region: attrs.region || '',
        country: attrs.country || '',
        
        // Investment criteria
        check_size_min: attrs['min-check-size'] || null,
        check_size_max: attrs['max-check-size'] || null,
        sweet_spot: attrs['sweet-spot-check-size'] || null,
        fund_size: attrs['recent-fund-size'] || null,
        fund_close_date: attrs['recent-fund-close-date'] || null,
        
        // Categories
        stages: attrs.stages || [],
        focus: attrs.focus || [],
        tags: attrs.tags || [],
        fund_types: attrs['fund-types'] || [],
        
        // Investment preferences
        leads_investments: attrs['leads-investments'],
        co_invests: attrs['co-invests'],
        takes_board_seats: attrs['takes-board-seats'],
        
        // Thesis & requirements
        thesis: (attrs.thesis || '').replace(/[\r\n]+/g, ' ').trim(),
        traction_metrics: (attrs['traction-metrics-requirements'] || '').replace(/[\r\n]+/g, ' ').trim(),
        investment_geography: attrs['investment-geography'] || [],
        
        // Social links
        twitter_url: attrs['twitter-url'] || '',
        linkedin_url: attrs['linkedin-url'] || '',
        
        // Meta
        verified: attrs.verified || false,
        badges: attrs.badges || [],
        profile_url: `https://connect.visible.vc/investors/${attrs.slug || data.id}`,
        
        scraped_at: new Date().toISOString(),
    };
}

function extractTeamMember(data, investorId, investorSlug) {
    const attrs = data.attributes || {};
    const avatarKey = attrs['external-image-s3-key'];
    return {
        id: data.id,
        investor_id: investorId,
        investor_slug: investorSlug,
        name: attrs.name || '',
        title: (attrs.title || '').trim(),
        linkedin_url: attrs['linkedin-url'] || '',
        twitter_url: attrs['twitter-url'] || '',
        avatar_url: avatarKey ? `https://d31s0k9giryrah.cloudfront.net/${avatarKey}` : '',
        scraped_at: new Date().toISOString(),
    };
}

function extractInvestment(data, investorId, investorSlug) {
    const attrs = data.attributes || {};
    return {
        id: data.id,
        investor_id: investorId,
        investor_slug: investorSlug,
        company_name: attrs.name || '',
        company_website: attrs.website || '',
        amount_raised: attrs['amount-raised'] ? parseFloat(attrs['amount-raised']) : null,
        announced_date: attrs['announced-date'] || '',
        company_city: attrs.city || '',
        company_country: attrs.country || '',
        article_url: attrs['article-url'] || '',
        scraped_at: new Date().toISOString(),
    };
}

// ============================================================================
// API Fetchers
// ============================================================================

async function fetchInvestorsList(offset, limit) {
    const endpoint = `${CONFIG.INVESTORS_ENDPOINT}?page[limit]=${limit}&page[offset]=${offset}`;
    return makeRequest(endpoint);
}

async function fetchInvestorBySlug(slug) {
    const endpoint = `${CONFIG.INVESTORS_ENDPOINT}?filter[slug]=${encodeURIComponent(slug)}`;
    const data = await makeRequest(endpoint);
    if (!data) return null; // Request was skipped
    return data.data?.[0] || null;
}

async function fetchTeamMembers(investorSlug) {
    const endpoint = `${CONFIG.PEOPLE_ENDPOINT}?filter[investor_slug]=${encodeURIComponent(investorSlug)}&page[limit]=50&page[offset]=0&sort=name`;
    const data = await makeRequest(endpoint);
    if (!data) return []; // Request was skipped
    return data.data || [];
}

async function fetchInvestments(investorSlug) {
    const endpoint = `${CONFIG.ROUNDS_ENDPOINT}?filter[investor_slug]=${encodeURIComponent(investorSlug)}&sort=-announced_date`;
    const data = await makeRequest(endpoint);
    if (!data) return []; // Request was skipped
    return data.data || [];
}

// ============================================================================
// Progress Management
// ============================================================================

function getProgressFile(outputDir) {
    return path.join(outputDir, '.progress.json');
}

function loadProgress(outputDir) {
    const progressFile = getProgressFile(outputDir);
    try {
        if (fs.existsSync(progressFile)) {
            return JSON.parse(fs.readFileSync(progressFile, 'utf8'));
        }
    } catch (e) {}
    return {
        offset: 0,
        processedSlugs: [],
        investors: [],
        teamMembers: [],
        investments: [],
        total: 0,
    };
}

function saveProgress(outputDir, progress) {
    const progressFile = getProgressFile(outputDir);
    fs.writeFileSync(progressFile, JSON.stringify(progress, null, 2));
}

function clearProgress(outputDir) {
    const progressFile = getProgressFile(outputDir);
    if (fs.existsSync(progressFile)) fs.unlinkSync(progressFile);
}

// ============================================================================
// Output Functions
// ============================================================================

function escapeCSV(value) {
    if (value === null || value === undefined) return '';
    if (Array.isArray(value)) value = value.join('; ');
    let str = String(value).replace(/[\r\n]+/g, ' ').trim();
    if (str.includes(',') || str.includes('"') || str.includes('\n')) {
        return `"${str.replace(/"/g, '""')}"`;
    }
    return str;
}

function arrayToCSV(data, columns) {
    const headers = columns.map(c => c.header || c.key);
    const rows = [headers.join(',')];
    
    for (const item of data) {
        const row = columns.map(c => escapeCSV(item[c.key]));
        rows.push(row.join(','));
    }
    
    return rows.join('\n');
}

function saveOutputs(outputDir, progress) {
    // Define column mappings for CSV
    const investorColumns = [
        { key: 'id', header: 'ID' },
        { key: 'name', header: 'Name' },
        { key: 'slug', header: 'Slug' },
        { key: 'website', header: 'Website' },
        { key: 'secondary_website', header: 'Secondary Website' },
        { key: 'description', header: 'Description' },
        { key: 'city', header: 'City' },
        { key: 'region', header: 'Region' },
        { key: 'country', header: 'Country' },
        { key: 'check_size_min', header: 'Check Size Min' },
        { key: 'check_size_max', header: 'Check Size Max' },
        { key: 'sweet_spot', header: 'Sweet Spot' },
        { key: 'fund_size', header: 'Fund Size' },
        { key: 'fund_close_date', header: 'Fund Close Date' },
        { key: 'stages', header: 'Stages' },
        { key: 'focus', header: 'Focus' },
        { key: 'tags', header: 'Tags' },
        { key: 'fund_types', header: 'Fund Types' },
        { key: 'leads_investments', header: 'Leads Investments' },
        { key: 'co_invests', header: 'Co-Invests' },
        { key: 'takes_board_seats', header: 'Takes Board Seats' },
        { key: 'thesis', header: 'Thesis' },
        { key: 'traction_metrics', header: 'Traction Metrics' },
        { key: 'investment_geography', header: 'Investment Geography' },
        { key: 'twitter_url', header: 'Twitter URL' },
        { key: 'linkedin_url', header: 'LinkedIn URL' },
        { key: 'verified', header: 'Verified' },
        { key: 'badges', header: 'Badges' },
        { key: 'profile_url', header: 'Profile URL' },
        { key: 'scraped_at', header: 'Scraped At' },
    ];

    const teamColumns = [
        { key: 'id', header: 'ID' },
        { key: 'investor_id', header: 'Investor ID' },
        { key: 'investor_slug', header: 'Investor Slug' },
        { key: 'name', header: 'Name' },
        { key: 'title', header: 'Title' },
        { key: 'linkedin_url', header: 'LinkedIn URL' },
        { key: 'twitter_url', header: 'Twitter URL' },
        { key: 'avatar_url', header: 'Avatar URL' },
        { key: 'scraped_at', header: 'Scraped At' },
    ];

    const investmentColumns = [
        { key: 'id', header: 'ID' },
        { key: 'investor_id', header: 'Investor ID' },
        { key: 'investor_slug', header: 'Investor Slug' },
        { key: 'company_name', header: 'Company Name' },
        { key: 'company_website', header: 'Company Website' },
        { key: 'amount_raised', header: 'Amount Raised' },
        { key: 'announced_date', header: 'Announced Date' },
        { key: 'company_city', header: 'Company City' },
        { key: 'company_country', header: 'Company Country' },
        { key: 'article_url', header: 'Article URL' },
        { key: 'scraped_at', header: 'Scraped At' },
    ];

    // Save investors
    fs.writeFileSync(
        path.join(outputDir, 'investors.json'),
        JSON.stringify(progress.investors, null, 2)
    );
    fs.writeFileSync(
        path.join(outputDir, 'investors.csv'),
        arrayToCSV(progress.investors, investorColumns)
    );

    // Save team members
    fs.writeFileSync(
        path.join(outputDir, 'team_members.json'),
        JSON.stringify(progress.teamMembers, null, 2)
    );
    fs.writeFileSync(
        path.join(outputDir, 'team_members.csv'),
        arrayToCSV(progress.teamMembers, teamColumns)
    );

    // Save investments
    fs.writeFileSync(
        path.join(outputDir, 'investments.json'),
        JSON.stringify(progress.investments, null, 2)
    );
    fs.writeFileSync(
        path.join(outputDir, 'investments.csv'),
        arrayToCSV(progress.investments, investmentColumns)
    );

    // Save full nested database
    const fullDatabase = {
        metadata: {
            source: 'https://connect.visible.vc/investors',
            scraped_at: new Date().toISOString(),
            counts: {
                investors: progress.investors.length,
                team_members: progress.teamMembers.length,
                investments: progress.investments.length,
            }
        },
        investors: progress.investors.map(inv => ({
            ...inv,
            team_members: progress.teamMembers.filter(tm => tm.investor_id === inv.id),
            investments: progress.investments.filter(i => i.investor_id === inv.id),
        }))
    };
    
    fs.writeFileSync(
        path.join(outputDir, 'full_database.json'),
        JSON.stringify(fullDatabase, null, 2)
    );

    // Save SQL schema helper
    const sqlSchema = `
-- Visible Connect Database Schema
-- Generated: ${new Date().toISOString()}
-- Source: https://connect.visible.vc/investors

-- Main investors table
CREATE TABLE IF NOT EXISTS investors (
    id VARCHAR(36) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    slug VARCHAR(255) UNIQUE,
    website VARCHAR(500),
    secondary_website VARCHAR(500),
    description TEXT,
    city VARCHAR(100),
    region VARCHAR(100),
    country VARCHAR(100),
    check_size_min BIGINT,
    check_size_max BIGINT,
    sweet_spot BIGINT,
    fund_size BIGINT,
    fund_close_date DATE,
    stages JSON,           -- Array: ["Seed", "Series A", ...]
    focus JSON,            -- Array: ["SaaS", "Fintech", ...]
    tags JSON,             -- Array: ["AI", "Big Data", ...]
    fund_types JSON,       -- Array: ["Venture Capital", ...]
    leads_investments BOOLEAN,
    co_invests BOOLEAN,
    takes_board_seats BOOLEAN,
    thesis TEXT,
    traction_metrics TEXT,
    investment_geography JSON,
    twitter_url VARCHAR(500),
    linkedin_url VARCHAR(500),
    verified BOOLEAN DEFAULT FALSE,
    badges JSON,
    profile_url VARCHAR(500),
    scraped_at TIMESTAMP
);

-- Team members at each investor firm
CREATE TABLE IF NOT EXISTS team_members (
    id VARCHAR(36) PRIMARY KEY,
    investor_id VARCHAR(36) REFERENCES investors(id) ON DELETE CASCADE,
    investor_slug VARCHAR(255),
    name VARCHAR(255) NOT NULL,
    title VARCHAR(255),
    linkedin_url VARCHAR(500),
    twitter_url VARCHAR(500),
    avatar_url VARCHAR(500),
    scraped_at TIMESTAMP
);

-- Investment rounds made by investors
CREATE TABLE IF NOT EXISTS investments (
    id VARCHAR(36) PRIMARY KEY,
    investor_id VARCHAR(36) REFERENCES investors(id) ON DELETE CASCADE,
    investor_slug VARCHAR(255),
    company_name VARCHAR(255) NOT NULL,
    company_website VARCHAR(500),
    amount_raised DECIMAL(15, 2),
    announced_date DATE,
    company_city VARCHAR(100),
    company_country VARCHAR(100),
    article_url VARCHAR(500),
    scraped_at TIMESTAMP
);

-- Useful indexes for common queries
CREATE INDEX idx_team_investor ON team_members(investor_id);
CREATE INDEX idx_investment_investor ON investments(investor_id);
CREATE INDEX idx_investor_city ON investors(city);
CREATE INDEX idx_investor_country ON investors(country);
CREATE INDEX idx_investor_verified ON investors(verified);
CREATE INDEX idx_investment_date ON investments(announced_date);
CREATE INDEX idx_investment_company ON investments(company_name);

-- Example queries:
-- Find all investors in San Francisco:
--   SELECT * FROM investors WHERE city = 'San Francisco';
--
-- Find all team members at a specific investor:
--   SELECT tm.* FROM team_members tm 
--   JOIN investors i ON tm.investor_id = i.id 
--   WHERE i.name = '01A (01 Advisors)';
--
-- Find top investors by number of investments:
--   SELECT i.name, COUNT(inv.id) as investment_count 
--   FROM investors i 
--   LEFT JOIN investments inv ON i.id = inv.investor_id 
--   GROUP BY i.id ORDER BY investment_count DESC LIMIT 20;
`;

    fs.writeFileSync(path.join(outputDir, 'schema.sql'), sqlSchema);

    console.log(`\n📁 Output files saved to: ${outputDir}/`);
    console.log(`   ├── investors.json/csv      (${progress.investors.length} records)`);
    console.log(`   ├── team_members.json/csv   (${progress.teamMembers.length} records)`);
    console.log(`   ├── investments.json/csv    (${progress.investments.length} records)`);
    console.log(`   ├── full_database.json      (nested structure)`);
    console.log(`   └── schema.sql              (database schema)`);
}

// ============================================================================
// Progress Display
// ============================================================================

function printProgress(current, total, startTime, phase) {
    const percent = Math.min(100, Math.round((current / total) * 100));
    const elapsed = (Date.now() - startTime) / 1000;
    const rate = current / elapsed || 0;
    const remaining = rate > 0 ? Math.round((total - current) / rate) : 0;
    
    const barWidth = 25;
    const filled = Math.round((percent / 100) * barWidth);
    const bar = '█'.repeat(filled) + '░'.repeat(barWidth - filled);
    
    const eta = remaining > 0 ? `${Math.floor(remaining / 60)}m ${remaining % 60}s` : '--';
    
    process.stdout.write(`\r[${bar}] ${percent}% | ${current}/${total} | ETA: ${eta} | ${phase}     `);
}

// ============================================================================
// Main Scraper
// ============================================================================

async function scrape(options) {
    console.log(`
╔══════════════════════════════════════════════════════════════════════════════╗
║               Visible Connect Full Database Scraper                          ║
║                                                                              ║
║  📊 Scraping investors, team members, and investment rounds                  ║
║  💾 Output: Normalized database tables (JSON, CSV, SQL schema)               ║
╚══════════════════════════════════════════════════════════════════════════════╝
`);

    // Setup output directory
    const outputDir = path.join(CONFIG.OUTPUT_DIR, options.output);
    if (!fs.existsSync(outputDir)) {
        fs.mkdirSync(outputDir, { recursive: true });
    }

    // Load or initialize progress
    let progress = options.resume ? loadProgress(outputDir) : {
        offset: 0,
        processedSlugs: [],
        allSlugs: [],  // Store all slugs for resume
        investors: [],
        teamMembers: [],
        investments: [],
        total: 0,
    };

    const isResuming = options.resume && progress.processedSlugs.length > 0;
    
    if (isResuming) {
        console.log(`📂 Resuming from checkpoint:`);
        console.log(`   - ${progress.processedSlugs.length} investors already scraped`);
        console.log(`   - ${progress.teamMembers.length} team members collected`);
        console.log(`   - ${progress.investments.length} investments collected\n`);
    }

    const startTime = Date.now();

    try {
        // Phase 1: Get investor list (or use cached list from progress)
        console.log('📋 Phase 1: Fetching investor list...\n');
        
        let allSlugs = progress.allSlugs || [];
        
        if (allSlugs.length === 0) {
            // Need to fetch the full list
            const firstPage = await fetchInvestorsList(0, CONFIG.PAGE_SIZE);
            const totalAvailable = firstPage.meta?.['record-count'] || firstPage.meta?.total || firstPage.data?.length || 0;
            progress.total = Math.min(totalAvailable, options.limit);

            console.log(`   Total available: ${totalAvailable.toLocaleString()}`);
            console.log(`   Will process: ${progress.total.toLocaleString()}`);
            console.log(`   Delay: ${options.delay}ms`);
            console.log(`   Rate limit protection: ${CONFIG.PAUSE_DURATION/1000}s pause every ${CONFIG.PAUSE_EVERY} investors\n`);

            // Collect all investor slugs
            allSlugs = firstPage.data.map(d => ({ id: d.id, slug: d.attributes?.slug }));
            
            let offset = CONFIG.PAGE_SIZE;
            while (allSlugs.length < progress.total) {
                printProgress(allSlugs.length, progress.total, startTime, 'Building list');
                await sleep(options.delay / 2);
                
                const page = await fetchInvestorsList(offset, CONFIG.PAGE_SIZE);
                if (!page.data?.length) break;
                
                for (const inv of page.data) {
                    if (allSlugs.length >= progress.total) break;
                    allSlugs.push({ id: inv.id, slug: inv.attributes?.slug });
                }
                offset += CONFIG.PAGE_SIZE;
            }
            
            // Save the full slug list for resume capability
            progress.allSlugs = allSlugs;
            progress.total = allSlugs.length;
            saveProgress(outputDir, progress);
            
            printProgress(allSlugs.length, progress.total, startTime, 'List complete');
            console.log('\n');
        } else {
            console.log(`   Using cached list of ${allSlugs.length} investors`);
            console.log(`   Already processed: ${progress.processedSlugs.length}`);
            console.log(`   Remaining: ${allSlugs.length - progress.processedSlugs.length}\n`);
        }

        // Phase 2: Fetch details for each investor
        console.log('🔍 Phase 2: Fetching investor details, team members & investments...\n');
        
        const detailStartTime = Date.now();
        const processedSet = new Set(progress.processedSlugs);  // Fast lookup
        let processed = progress.processedSlugs.length;
        let consecutiveInThisBatch = 0;

        for (const { id, slug } of allSlugs) {
            // Skip already processed
            if (processedSet.has(slug)) continue;
            if (processed >= progress.total) break;

            printProgress(processed + 1, progress.total, detailStartTime, `${slug.substring(0, 30)}...`);

            // Rate limit protection: take a short break periodically
            if (consecutiveInThisBatch > 0 && consecutiveInThisBatch % CONFIG.PAUSE_EVERY === 0) {
                saveProgress(outputDir, progress);
                saveOutputs(outputDir, progress);
                console.log(`\n💾 Checkpoint: ${processed}/${progress.total} investors | ${progress.teamMembers.length} team | ${progress.investments.length} investments`);
                await sleep(CONFIG.PAUSE_DURATION);
            }

            try {
                // Fetch investor details with conservative delay
                await sleep(options.delay);
                const investorData = await fetchInvestorBySlug(slug);
                
                if (investorData) {
                    const investor = extractInvestor(investorData);
                    progress.investors.push(investor);

                    // Fetch team members with delay
                    await sleep(CONFIG.MIN_DELAY_BETWEEN_CALLS);
                    const teamData = await fetchTeamMembers(slug);
                    if (teamData && Array.isArray(teamData)) {
                        for (const member of teamData) {
                            progress.teamMembers.push(extractTeamMember(member, investor.id, slug));
                        }
                    }

                    // Fetch investments with delay
                    await sleep(CONFIG.MIN_DELAY_BETWEEN_CALLS);
                    const roundsData = await fetchInvestments(slug);
                    if (roundsData && Array.isArray(roundsData)) {
                        for (const round of roundsData) {
                            progress.investments.push(extractInvestment(round, investor.id, slug));
                        }
                    }
                }

                progress.processedSlugs.push(slug);
                processedSet.add(slug);
                processed++;
                consecutiveInThisBatch++;

            } catch (error) {
                // This should rarely happen now since makeRequest handles all errors
                // But just in case, log and continue
                console.log(`\n⚠️  Unexpected error for ${slug}: ${error.message} - marking as processed and continuing`);
                progress.processedSlugs.push(slug);
                processedSet.add(slug);
                processed++;
                consecutiveInThisBatch++;
            }

            // Save progress frequently
            if (processed % CONFIG.SAVE_EVERY === 0) {
                saveProgress(outputDir, progress);
            }
        }

        printProgress(processed, progress.total, detailStartTime, 'Complete!');
        console.log('\n');

        // Save final outputs
        saveOutputs(outputDir, progress);
        clearProgress(outputDir);

        // Summary
        const elapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
        console.log(`
╔══════════════════════════════════════════════════════════════════════════════╗
║                              ✅ Scraping Complete!                           ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  Investors:     ${String(progress.investors.length).padEnd(58)}║
║  Team Members:  ${String(progress.teamMembers.length).padEnd(58)}║
║  Investments:   ${String(progress.investments.length).padEnd(58)}║
║  Time elapsed:  ${(elapsed + ' minutes').padEnd(58)}║
╚══════════════════════════════════════════════════════════════════════════════╝
`);

    } catch (error) {
        // This should almost never happen now, but if it does, save and continue
        console.error(`\n\n⚠️  Unexpected error in main loop: ${error.message}`);
        console.error(error.stack);
        
        if (progress.processedSlugs.length > 0) {
            saveProgress(outputDir, progress);
            saveOutputs(outputDir, progress);
            console.log(`\n💾 Progress saved (${progress.processedSlugs.length} investors).`);
        }
        
        // Wait and retry the entire scrape instead of crashing
        console.log(`\n🔄 Waiting 5 minutes before retrying...`);
        await sleep(300000); // 5 minute wait
        console.log(`\n▶️  Retrying scrape with --resume...\n`);
        
        // Recursive retry with resume
        options.resume = true;
        return scrape(options);
    }
}

// ============================================================================
// Entry Point
// ============================================================================

const options = parseArgs();
scrape(options);

