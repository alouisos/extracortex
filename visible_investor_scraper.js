#!/usr/bin/env node
/**
 * Visible Connect Investor Scraper
 * 
 * A safe, rate-limited scraper for https://connect.visible.vc/investors
 * Uses the public API endpoint with respectful delays to avoid overwhelming the server.
 * 
 * Usage:
 *   node visible_investor_scraper.js [options]
 * 
 * Options:
 *   --limit <n>      Maximum number of investors to fetch (default: all ~19,300)
 *   --delay <ms>     Delay between requests in milliseconds (default: 1000)
 *   --output <file>  Output filename without extension (default: visible_investors)
 *   --format <type>  Output format: json, csv, or both (default: both)
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
    FILTER_OPTIONS_ENDPOINT: '/investor_filter_options',
    INVESTORS_ENDPOINT: '/investors',
    PAGE_SIZE: 100,           // Max items per request (API limit)
    DEFAULT_DELAY: 1000,      // 1 second between requests (be respectful)
    DETAIL_DELAY: 300,        // Shorter delay for detail fetches (still respectful)
    MAX_RETRIES: 3,           // Retry failed requests
    RETRY_DELAY: 5000,        // 5 seconds before retry
    OUTPUT_DIR: path.join(__dirname, 'results'),
    PROGRESS_FILE: path.join(__dirname, 'results', '.scraper_progress.json'),
    USER_AGENT: 'Mozilla/5.0 (compatible; VisibleInvestorScraper/1.0; educational purposes)',
};

// ============================================================================
// Utility Functions
// ============================================================================

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

function parseArgs() {
    const args = process.argv.slice(2);
    const options = {
        limit: Infinity,
        delay: CONFIG.DEFAULT_DELAY,
        output: 'visible_investors',
        format: 'both',
        resume: false,
        detailed: false,  // Fetch full details for each investor
    };

    for (let i = 0; i < args.length; i++) {
        switch (args[i]) {
            case '--limit':
                options.limit = parseInt(args[++i], 10);
                break;
            case '--delay':
                options.delay = parseInt(args[++i], 10);
                break;
            case '--output':
                options.output = args[++i];
                break;
            case '--format':
                options.format = args[++i];
                break;
            case '--resume':
                options.resume = true;
                break;
            case '--detailed':
                options.detailed = true;
                break;
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
╔════════════════════════════════════════════════════════════════════════════╗
║                     Visible Connect Investor Scraper                       ║
╚════════════════════════════════════════════════════════════════════════════╝

A safe, rate-limited scraper for https://connect.visible.vc/investors

USAGE:
  node visible_investor_scraper.js [options]

OPTIONS:
  --limit <n>      Maximum number of investors to fetch (default: all ~19,300)
  --delay <ms>     Delay between requests in milliseconds (default: 1000)
  --output <file>  Output filename without extension (default: visible_investors)
  --format <type>  Output format: json, csv, or both (default: both)
  --resume         Resume from last saved progress
  --detailed       Fetch full details for each investor (slower but more data)
  --help, -h       Show this help message

EXAMPLES:
  # Fetch first 100 investors (basic info)
  node visible_investor_scraper.js --limit 100

  # Fetch first 50 with full details (location, check size, focuses)
  node visible_investor_scraper.js --limit 50 --detailed

  # Fetch all with 2 second delay
  node visible_investor_scraper.js --delay 2000

  # Resume interrupted scrape
  node visible_investor_scraper.js --resume

  # Custom output file
  node visible_investor_scraper.js --output my_investors --format csv
`);
}

// ============================================================================
// HTTP Request Handler
// ============================================================================

function makeRequest(endpoint, retryCount = 0) {
    return new Promise((resolve, reject) => {
        const url = `https://${CONFIG.API_BASE}${endpoint}`;
        
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
                        reject(new Error(`Failed to parse JSON: ${e.message}`));
                    }
                } else if (res.statusCode === 429) {
                    // Rate limited - wait and retry
                    if (retryCount < CONFIG.MAX_RETRIES) {
                        console.log(`⚠️  Rate limited. Waiting ${CONFIG.RETRY_DELAY / 1000}s before retry...`);
                        sleep(CONFIG.RETRY_DELAY).then(() => {
                            resolve(makeRequest(endpoint, retryCount + 1));
                        });
                    } else {
                        reject(new Error('Rate limit exceeded after max retries'));
                    }
                } else {
                    reject(new Error(`HTTP ${res.statusCode}: ${data.substring(0, 200)}`));
                }
            });
        });

        req.on('error', (error) => {
            if (retryCount < CONFIG.MAX_RETRIES) {
                console.log(`⚠️  Request failed: ${error.message}. Retrying...`);
                sleep(CONFIG.RETRY_DELAY).then(() => {
                    resolve(makeRequest(endpoint, retryCount + 1));
                });
            } else {
                reject(error);
            }
        });

        req.setTimeout(30000, () => {
            req.destroy();
            reject(new Error('Request timeout'));
        });

        req.end();
    });
}

// ============================================================================
// Data Extraction
// ============================================================================

function extractInvestorData(investor) {
    const attrs = investor.attributes || {};
    
    return {
        id: investor.id,
        name: attrs.name || '',
        slug: attrs.slug || '',
        website: attrs.website || '',
        location: attrs.location || '',
        check_size_min: attrs['check-size-min'] || null,
        check_size_max: attrs['check-size-max'] || null,
        check_size_display: attrs['check-size'] || '',
        stages: (attrs.stages || []).join(', '),
        stages_list: attrs.stages || [],
        focuses: (attrs.focuses || []).join(', '),
        focuses_list: attrs.focuses || [],
        verified: attrs.verified || false,
        partner: attrs['partner-name'] || '',
        logo_url: attrs['logo-url'] || '',
        profile_url: `https://connect.visible.vc/investors/${attrs.slug || investor.id}`,
        scraped_at: new Date().toISOString(),
    };
}

function extractDetailedInvestorData(investor, basicData) {
    const attrs = investor.attributes || {};
    
    // Merge with basic data, overwriting with detailed data where available
    return {
        ...basicData,
        // Detailed fields that come from individual investor endpoint
        location: attrs.location || basicData.location || '',
        city: attrs.city || '',
        country: attrs.country || '',
        check_size_min: attrs['check-size-min'] || basicData.check_size_min,
        check_size_max: attrs['check-size-max'] || basicData.check_size_max,
        check_size_display: formatCheckSize(attrs['check-size-min'], attrs['check-size-max']) || basicData.check_size_display,
        focuses: (attrs.focuses || []).join(', ') || basicData.focuses,
        focuses_list: attrs.focuses || basicData.focuses_list || [],
        description: attrs.description || '',
        twitter: attrs.twitter || '',
        linkedin: attrs['linkedin-url'] || '',
        crunchbase: attrs['crunchbase-url'] || '',
        angellist: attrs['angellist-url'] || '',
        // Additional metadata
        has_detailed_data: true,
    };
}

function formatCheckSize(min, max) {
    if (!min && !max) return '';
    
    const formatAmount = (amount) => {
        if (!amount) return '';
        if (amount >= 1000000) return `$${(amount / 1000000).toFixed(amount % 1000000 === 0 ? 0 : 1)}M`;
        if (amount >= 1000) return `$${(amount / 1000).toFixed(0)}K`;
        return `$${amount}`;
    };
    
    if (min && max) return `${formatAmount(min)} - ${formatAmount(max)}`;
    if (min) return `> ${formatAmount(min)}`;
    if (max) return `< ${formatAmount(max)}`;
    return '';
}

async function fetchInvestorDetails(slug) {
    try {
        const data = await makeRequest(`${CONFIG.INVESTORS_ENDPOINT}?filter[slug]=${encodeURIComponent(slug)}`);
        // The API returns an array, get the first item
        const investors = data.data || [];
        return investors.length > 0 ? investors[0] : null;
    } catch (error) {
        console.log(`\n⚠️  Could not fetch details for ${slug}: ${error.message}`);
        return null;
    }
}

// ============================================================================
// Progress Management
// ============================================================================

function loadProgress() {
    try {
        if (fs.existsSync(CONFIG.PROGRESS_FILE)) {
            const data = fs.readFileSync(CONFIG.PROGRESS_FILE, 'utf8');
            return JSON.parse(data);
        }
    } catch (e) {
        console.log('⚠️  Could not load progress file, starting fresh');
    }
    return { offset: 0, investors: [], total: 0 };
}

function saveProgress(progress) {
    fs.writeFileSync(CONFIG.PROGRESS_FILE, JSON.stringify(progress, null, 2));
}

function clearProgress() {
    if (fs.existsSync(CONFIG.PROGRESS_FILE)) {
        fs.unlinkSync(CONFIG.PROGRESS_FILE);
    }
}

// ============================================================================
// Output Functions
// ============================================================================

function saveToJSON(investors, filename) {
    const filepath = path.join(CONFIG.OUTPUT_DIR, `${filename}.json`);
    const output = {
        metadata: {
            source: 'https://connect.visible.vc/investors',
            scraped_at: new Date().toISOString(),
            total_investors: investors.length,
        },
        investors: investors
    };
    fs.writeFileSync(filepath, JSON.stringify(output, null, 2));
    console.log(`✅ Saved JSON: ${filepath}`);
    return filepath;
}

function saveToCSV(investors, filename) {
    const filepath = path.join(CONFIG.OUTPUT_DIR, `${filename}.csv`);
    
    const headers = [
        'ID',
        'Name',
        'Website',
        'Location',
        'City',
        'Country',
        'Check Size Min',
        'Check Size Max',
        'Check Size Display',
        'Stages',
        'Focuses',
        'Verified',
        'Partner',
        'Description',
        'Twitter',
        'LinkedIn',
        'Crunchbase',
        'AngelList',
        'Logo URL',
        'Profile URL',
        'Scraped At'
    ];

    const escapeCSV = (value) => {
        if (value === null || value === undefined) return '';
        let str = String(value);
        // Replace newlines with spaces for CSV compatibility
        str = str.replace(/[\r\n]+/g, ' ').trim();
        if (str.includes(',') || str.includes('"')) {
            return `"${str.replace(/"/g, '""')}"`;
        }
        return str;
    };

    const rows = [headers.join(',')];
    
    for (const inv of investors) {
        const row = [
            escapeCSV(inv.id),
            escapeCSV(inv.name),
            escapeCSV(inv.website),
            escapeCSV(inv.location),
            escapeCSV(inv.city),
            escapeCSV(inv.country),
            escapeCSV(inv.check_size_min),
            escapeCSV(inv.check_size_max),
            escapeCSV(inv.check_size_display),
            escapeCSV(inv.stages),
            escapeCSV(inv.focuses),
            escapeCSV(inv.verified),
            escapeCSV(inv.partner),
            escapeCSV(inv.description),
            escapeCSV(inv.twitter),
            escapeCSV(inv.linkedin),
            escapeCSV(inv.crunchbase),
            escapeCSV(inv.angellist),
            escapeCSV(inv.logo_url),
            escapeCSV(inv.profile_url),
            escapeCSV(inv.scraped_at)
        ];
        rows.push(row.join(','));
    }

    fs.writeFileSync(filepath, rows.join('\n'));
    console.log(`✅ Saved CSV: ${filepath}`);
    return filepath;
}

// ============================================================================
// Progress Bar
// ============================================================================

function printProgress(current, total, startTime, label = '') {
    const percent = Math.round((current / total) * 100);
    const elapsed = (Date.now() - startTime) / 1000;
    const rate = current / elapsed;
    const remaining = Math.round((total - current) / rate);
    
    const barWidth = 25;
    const filled = Math.round((percent / 100) * barWidth);
    const empty = barWidth - filled;
    const bar = '█'.repeat(filled) + '░'.repeat(empty);
    
    const eta = remaining > 0 && isFinite(remaining)
        ? `${Math.floor(remaining / 60)}m ${remaining % 60}s` 
        : '0s';
    
    const labelStr = label ? ` ${label}` : '';
    
    process.stdout.write(
        `\r[${bar}] ${percent}% | ${current}/${total} | ETA: ${eta}${labelStr}     `
    );
}

// ============================================================================
// Main Scraper
// ============================================================================

async function scrapeInvestors(options) {
    console.log(`
╔════════════════════════════════════════════════════════════════════════════╗
║                     Visible Connect Investor Scraper                       ║
║                                                                            ║
║  ⚠️  This scraper respects rate limits and uses reasonable delays.         ║
║  📋 Data is from the public API at connect.visible.vc                      ║
╚════════════════════════════════════════════════════════════════════════════╝
`);

    // Ensure output directory exists
    if (!fs.existsSync(CONFIG.OUTPUT_DIR)) {
        fs.mkdirSync(CONFIG.OUTPUT_DIR, { recursive: true });
    }

    // Load or initialize progress
    let progress;
    if (options.resume) {
        progress = loadProgress();
        if (progress.investors.length > 0) {
            console.log(`📂 Resuming from offset ${progress.offset} (${progress.investors.length} investors already scraped)`);
        }
    } else {
        progress = { offset: 0, investors: [], total: 0, detailsProcessed: 0 };
    }

    const startTime = Date.now();
    
    try {
        // First request to get total count
        console.log('🔍 Fetching initial data...\n');
        const firstPage = await makeRequest(
            `${CONFIG.INVESTORS_ENDPOINT}?page[limit]=${CONFIG.PAGE_SIZE}&page[offset]=${progress.offset}`
        );
        
        const totalAvailable = firstPage.meta?.total || firstPage.data?.length || 0;
        progress.total = Math.min(totalAvailable, options.limit);
        
        console.log(`📊 Total investors available: ${totalAvailable.toLocaleString()}`);
        console.log(`🎯 Will fetch: ${progress.total.toLocaleString()} investors`);
        console.log(`⏱️  Delay between requests: ${options.delay}ms`);
        console.log(`🔎 Detailed mode: ${options.detailed ? 'ON (fetching full profiles)' : 'OFF (basic info only)'}`);
        console.log(`📁 Output: ${options.output}.{json,csv}\n`);

        // Process first page
        if (progress.offset === 0) {
            for (const investor of firstPage.data || []) {
                progress.investors.push(extractInvestorData(investor));
                if (progress.investors.length >= options.limit) break;
            }
        }

        // Fetch remaining pages
        while (progress.investors.length < progress.total) {
            progress.offset += CONFIG.PAGE_SIZE;
            
            printProgress(progress.investors.length, progress.total, startTime, 'Fetching list');
            
            await sleep(options.delay);
            
            const page = await makeRequest(
                `${CONFIG.INVESTORS_ENDPOINT}?page[limit]=${CONFIG.PAGE_SIZE}&page[offset]=${progress.offset}`
            );
            
            if (!page.data || page.data.length === 0) {
                console.log('\n⚠️  No more data available');
                break;
            }
            
            for (const investor of page.data) {
                progress.investors.push(extractInvestorData(investor));
                if (progress.investors.length >= options.limit) break;
            }
            
            // Save progress periodically (every 500 investors)
            if (progress.investors.length % 500 === 0) {
                saveProgress(progress);
            }
        }

        printProgress(progress.investors.length, progress.total, startTime, 'List complete');
        console.log('\n');

        // If detailed mode, fetch individual profiles
        if (options.detailed) {
            console.log('📋 Fetching detailed profiles...\n');
            const detailStartTime = Date.now();
            
            for (let i = progress.detailsProcessed || 0; i < progress.investors.length; i++) {
                const investor = progress.investors[i];
                
                if (investor.has_detailed_data) continue; // Skip if already has details
                
                printProgress(i + 1, progress.investors.length, detailStartTime, 'Fetching details');
                
                await sleep(CONFIG.DETAIL_DELAY);
                
                const details = await fetchInvestorDetails(investor.slug);
                if (details) {
                    progress.investors[i] = extractDetailedInvestorData(details, investor);
                }
                
                progress.detailsProcessed = i + 1;
                
                // Save progress periodically
                if ((i + 1) % 100 === 0) {
                    saveProgress(progress);
                }
            }
            
            printProgress(progress.investors.length, progress.investors.length, detailStartTime, 'Details complete');
            console.log('\n');
        }

        // Save final results
        const timestamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
        const outputName = `${options.output}_${timestamp}`;

        if (options.format === 'json' || options.format === 'both') {
            saveToJSON(progress.investors, outputName);
        }
        if (options.format === 'csv' || options.format === 'both') {
            saveToCSV(progress.investors, outputName);
        }

        // Clear progress file on successful completion
        clearProgress();

        // Print summary
        const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
        console.log(`
╔════════════════════════════════════════════════════════════════════════════╗
║                              ✅ Complete!                                  ║
╠════════════════════════════════════════════════════════════════════════════╣
║  Investors scraped: ${String(progress.investors.length).padEnd(52)}║
║  Detailed profiles: ${String(options.detailed ? progress.investors.length : 0).padEnd(52)}║
║  Time elapsed: ${(elapsed + 's').padEnd(57)}║
║  Output directory: ${CONFIG.OUTPUT_DIR.substring(0, 53).padEnd(53)}║
╚════════════════════════════════════════════════════════════════════════════╝
`);

    } catch (error) {
        console.error(`\n❌ Error: ${error.message}`);
        
        // Save progress on error so we can resume
        if (progress.investors.length > 0) {
            saveProgress(progress);
            console.log(`💾 Progress saved. Run with --resume to continue.`);
        }
        
        process.exit(1);
    }
}

// ============================================================================
// Entry Point
// ============================================================================

const options = parseArgs();
scrapeInvestors(options);

