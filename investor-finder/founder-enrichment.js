#!/usr/bin/env node
/**
 * Founder Enrichment Script v2
 * 
 * Finds founders and their LinkedIn profiles for portfolio companies
 * using AI-powered web search and extraction.
 * 
 * Features:
 * - Multiple search strategies to avoid rate limiting
 * - Saves to both local JSON and integrates with main database
 * - Smart caching and resume support
 * - Rotating user agents and request patterns
 * 
 * Usage:
 *   node founder-enrichment.js [--limit N] [--since YYYY-MM-DD] [--batch N]
 * 
 * Examples:
 *   node founder-enrichment.js --limit 10          # Test with 10 companies
 *   node founder-enrichment.js --since 2025-01-01  # All 2025 investments
 *   node founder-enrichment.js --batch 50          # Process in batches of 50
 */

const fs = require('fs');
const path = require('path');
require('dotenv').config();

// OpenAI setup
const OpenAI = require('openai');
const openai = new OpenAI({
    apiKey: process.env.OPENAI_API_KEY
});

// Configuration
const CONFIG = {
    // Data paths
    INVESTMENTS_PATH: path.join(__dirname, '../results/unified_database/investments.json'),
    FOUNDERS_PATH: path.join(__dirname, '../results/unified_database/founders.json'),
    PORTFOLIO_FOUNDERS_PATH: path.join(__dirname, '../results/unified_database/portfolio_founders.json'),
    PROGRESS_PATH: path.join(__dirname, '../results/unified_database/founder_enrichment_progress.json'),
    
    // Rate limiting - randomized to appear more human
    MIN_DELAY: 500,    // Minimum delay between requests (ms)
    MAX_DELAY: 1500,   // Maximum delay between requests (ms)
    BATCH_PAUSE: 5000, // Pause between batches (ms)
    BATCH_SIZE: 20,    // Companies per batch before longer pause
    
    // Defaults
    DEFAULT_SINCE_DATE: '2025-01-01',
    DEFAULT_LIMIT: null,
};

// Rotating user agents to avoid detection
const USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
];

function getRandomUserAgent() {
    return USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];
}

function getRandomDelay() {
    return CONFIG.MIN_DELAY + Math.random() * (CONFIG.MAX_DELAY - CONFIG.MIN_DELAY);
}

// ============================================================================
// Web scraping with multiple strategies
// ============================================================================

let fetchModule = null;
async function getFetch() {
    if (!fetchModule) {
        fetchModule = (await import('node-fetch')).default;
    }
    return fetchModule;
}

// Strategy 1: Direct company website scraping
async function scrapeCompanyWebsite(websiteUrl) {
    if (!websiteUrl) return null;
    
    const fetch = await getFetch();
    
    try {
        // Try common about/team pages
        const pagesToTry = [
            websiteUrl,
            websiteUrl.replace(/\/$/, '') + '/about',
            websiteUrl.replace(/\/$/, '') + '/team',
            websiteUrl.replace(/\/$/, '') + '/about-us',
            websiteUrl.replace(/\/$/, '') + '/company',
        ];
        
        let combinedContent = '';
        
        for (const pageUrl of pagesToTry.slice(0, 3)) { // Try first 3
            try {
                const response = await fetch(pageUrl, {
                    headers: {
                        'User-Agent': getRandomUserAgent(),
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                        'Accept-Language': 'en-US,en;q=0.9',
                    },
                    timeout: 10000,
                });
                
                if (response.ok) {
                    const html = await response.text();
                    combinedContent += html.substring(0, 50000) + '\n\n';
                }
            } catch (e) {
                // Continue to next page
            }
            await sleep(getRandomDelay());
        }
        
        return combinedContent || null;
    } catch (error) {
        return null;
    }
}

// Strategy 2: DuckDuckGo search (less aggressive rate limiting than Google)
async function duckDuckGoSearch(query) {
    const fetch = await getFetch();
    
    const encodedQuery = encodeURIComponent(query);
    const url = `https://html.duckduckgo.com/html/?q=${encodedQuery}`;
    
    try {
        const response = await fetch(url, {
            headers: {
                'User-Agent': getRandomUserAgent(),
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
            },
            timeout: 15000,
        });
        
        if (!response.ok) {
            return null;
        }
        
        return await response.text();
    } catch (error) {
        return null;
    }
}

// Strategy 3: Bing search (alternative to Google)
async function bingSearch(query) {
    const fetch = await getFetch();
    
    const encodedQuery = encodeURIComponent(query);
    const url = `https://www.bing.com/search?q=${encodedQuery}&count=20`;
    
    try {
        const response = await fetch(url, {
            headers: {
                'User-Agent': getRandomUserAgent(),
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
            },
            timeout: 15000,
        });
        
        if (!response.ok) {
            return null;
        }
        
        return await response.text();
    } catch (error) {
        return null;
    }
}

// ============================================================================
// AI-powered founder extraction (using OpenAI's web browsing capability)
// ============================================================================

async function extractFoundersWithAI(companyName, companyWebsite, webContent) {
    const prompt = `You are analyzing web content to find the founders of a startup company.

Company: ${companyName}
Website: ${companyWebsite || 'Unknown'}

Web Content (from company website and search results):
${webContent?.substring(0, 12000) || 'No content available'}

Based on the content, identify the founders/co-founders of this company.

Return a JSON object with this structure:
{
  "founders": [
    {
      "name": "Full Name",
      "title": "CEO/Founder/Co-founder/CTO etc",
      "linkedin_url": "https://linkedin.com/in/username or null if not found"
    }
  ],
  "confidence": "high/medium/low",
  "source": "Brief description of where you found this info"
}

Rules:
- Only include actual founders/co-founders, not employees or investors
- LinkedIn URLs must be in format https://linkedin.com/in/username or https://www.linkedin.com/in/username
- If you can't find LinkedIn URLs, set them to null
- If you can't find any founders, return empty founders array
- Deduplicate - don't include the same person twice
- Be conservative - only include people you're confident are founders`;

    try {
        const response = await openai.chat.completions.create({
            model: 'gpt-4o-mini',
            messages: [
                { role: 'system', content: 'You extract structured data from web content. Always respond with valid JSON.' },
                { role: 'user', content: prompt }
            ],
            temperature: 0.1,
            response_format: { type: 'json_object' }
        });
        
        const content = response.choices[0].message.content;
        const result = JSON.parse(content);
        
        // Deduplicate founders by name
        const seen = new Set();
        result.founders = (result.founders || []).filter(f => {
            const key = f.name?.toLowerCase();
            if (!key || seen.has(key)) return false;
            seen.add(key);
            return true;
        });
        
        return result;
    } catch (error) {
        console.error(`   ⚠️ AI extraction error: ${error.message}`);
        return { founders: [], confidence: 'failed', source: error.message };
    }
}

// Use OpenAI to search and find founders directly (most reliable)
async function findFoundersWithOpenAI(companyName, companyWebsite) {
    const prompt = `Find the founders/co-founders of the startup company "${companyName}"${companyWebsite ? ` (website: ${companyWebsite})` : ''}.

Search your knowledge to identify who founded this company.

Return a JSON object:
{
  "founders": [
    {
      "name": "Full Name",
      "title": "CEO/Founder/Co-founder/CTO etc", 
      "linkedin_url": "https://linkedin.com/in/username or null"
    }
  ],
  "confidence": "high/medium/low",
  "source": "Your knowledge source"
}

Rules:
- Only include actual founders/co-founders
- For LinkedIn URLs, use the format https://www.linkedin.com/in/username
- If unsure about LinkedIn URL, set to null
- If you don't know the founders, return empty array with low confidence`;

    try {
        const response = await openai.chat.completions.create({
            model: 'gpt-4o',  // Use full model for better knowledge
            messages: [
                { role: 'system', content: 'You are a startup research assistant with extensive knowledge of tech companies and their founders.' },
                { role: 'user', content: prompt }
            ],
            temperature: 0.1,
            response_format: { type: 'json_object' }
        });
        
        const content = response.choices[0].message.content;
        const result = JSON.parse(content);
        
        // Deduplicate
        const seen = new Set();
        result.founders = (result.founders || []).filter(f => {
            const key = f.name?.toLowerCase();
            if (!key || seen.has(key)) return false;
            seen.add(key);
            return true;
        });
        
        return result;
    } catch (error) {
        console.error(`   ⚠️ OpenAI search error: ${error.message}`);
        return { founders: [], confidence: 'failed', source: error.message };
    }
}

// ============================================================================
// LinkedIn search for missing profiles
// ============================================================================

async function searchLinkedIn(founderName, companyName) {
    // Try DuckDuckGo first (less rate limiting)
    const query = `${founderName} ${companyName} site:linkedin.com/in`;
    
    let html = await duckDuckGoSearch(query);
    await sleep(getRandomDelay());
    
    if (!html) {
        // Fallback to Bing
        html = await bingSearch(query);
        await sleep(getRandomDelay());
    }
    
    if (!html) return null;
    
    // Extract LinkedIn URLs from search results
    const linkedInRegex = /https?:\/\/(?:www\.)?linkedin\.com\/in\/[a-zA-Z0-9_-]+/g;
    const matches = html.match(linkedInRegex);
    
    if (matches && matches.length > 0) {
        // Clean and return the first match
        let url = matches[0];
        if (!url.startsWith('https://www.')) {
            url = url.replace('https://linkedin.com', 'https://www.linkedin.com');
            url = url.replace('http://linkedin.com', 'https://www.linkedin.com');
            url = url.replace('http://www.linkedin.com', 'https://www.linkedin.com');
        }
        return url;
    }
    
    return null;
}

// ============================================================================
// Main enrichment logic
// ============================================================================

async function enrichCompany(company) {
    console.log(`\n📊 Processing: ${company.company_name}`);
    console.log(`   Website: ${company.company_website || 'N/A'}`);
    
    let extraction = { founders: [], confidence: 'none' };
    
    // Strategy 1: Use OpenAI's knowledge directly (fastest, no rate limiting)
    console.log(`   🤖 Querying AI knowledge base...`);
    extraction = await findFoundersWithOpenAI(company.company_name, company.company_website);
    
    // Strategy 2: If AI doesn't know, try scraping company website
    if (extraction.founders.length === 0 && company.company_website) {
        console.log(`   🌐 Scraping company website...`);
        const websiteContent = await scrapeCompanyWebsite(company.company_website);
        
        if (websiteContent) {
            extraction = await extractFoundersWithAI(
                company.company_name,
                company.company_website,
                websiteContent
            );
        }
    }
    
    // Strategy 3: If still no results, try search engines
    if (extraction.founders.length === 0) {
        console.log(`   🔍 Searching DuckDuckGo...`);
        const searchQuery = `"${company.company_name}" founder co-founder CEO`;
        const searchResults = await duckDuckGoSearch(searchQuery);
        await sleep(getRandomDelay());
        
        if (searchResults) {
            extraction = await extractFoundersWithAI(
                company.company_name,
                company.company_website,
                searchResults
            );
        }
    }
    
    // Try to find LinkedIn URLs for founders without them
    for (const founder of extraction.founders) {
        if (!founder.linkedin_url && founder.name) {
            console.log(`   🔗 Searching LinkedIn for: ${founder.name}`);
            founder.linkedin_url = await searchLinkedIn(founder.name, company.company_name);
        }
    }
    
    const result = {
        company_name: company.company_name,
        company_website: company.company_website,
        investor_id: company.investor_id,
        investor_slug: company.investor_slug,
        announced_date: company.announced_date,
        amount_raised: company.amount_raised,
        founders: extraction.founders,
        confidence: extraction.confidence,
        source: extraction.source,
        enriched_at: new Date().toISOString()
    };
    
    const founderCount = extraction.founders.length;
    const linkedInCount = extraction.founders.filter(f => f.linkedin_url).length;
    
    console.log(`   ✅ Found ${founderCount} founders (${extraction.confidence} confidence), ${linkedInCount} with LinkedIn`);
    for (const f of extraction.founders) {
        console.log(`      - ${f.name} (${f.title}) ${f.linkedin_url ? '✓ LinkedIn' : '✗ No LinkedIn'}`);
    }
    
    return result;
}

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

// ============================================================================
// Progress tracking and data persistence
// ============================================================================

function loadProgress() {
    try {
        if (fs.existsSync(CONFIG.PROGRESS_PATH)) {
            return JSON.parse(fs.readFileSync(CONFIG.PROGRESS_PATH, 'utf8'));
        }
    } catch (e) {}
    return { processed: [], lastRun: null, stats: { total: 0, withFounders: 0, withLinkedIn: 0 } };
}

function saveProgress(progress) {
    fs.writeFileSync(CONFIG.PROGRESS_PATH, JSON.stringify(progress, null, 2));
}

function loadFounders() {
    try {
        if (fs.existsSync(CONFIG.FOUNDERS_PATH)) {
            return JSON.parse(fs.readFileSync(CONFIG.FOUNDERS_PATH, 'utf8'));
        }
    } catch (e) {}
    return [];
}

function saveFounders(founders) {
    // Save to main founders.json
    fs.writeFileSync(CONFIG.FOUNDERS_PATH, JSON.stringify(founders, null, 2));
    
    // Also save a flattened version for easy database import
    // This creates a table-like structure: one row per founder
    const flatFounders = [];
    for (const company of founders) {
        for (const founder of company.founders || []) {
            flatFounders.push({
                id: `${company.company_name}-${founder.name}`.toLowerCase().replace(/[^a-z0-9]/g, '-'),
                company_name: company.company_name,
                company_website: company.company_website,
                investor_id: company.investor_id,
                investor_slug: company.investor_slug,
                founder_name: founder.name,
                founder_title: founder.title,
                founder_linkedin: founder.linkedin_url,
                confidence: company.confidence,
                announced_date: company.announced_date,
                amount_raised: company.amount_raised,
                enriched_at: company.enriched_at
            });
        }
    }
    
    fs.writeFileSync(CONFIG.PORTFOLIO_FOUNDERS_PATH, JSON.stringify(flatFounders, null, 2));
    
    console.log(`   💾 Saved ${founders.length} companies, ${flatFounders.length} founder records`);
}

// Create SQL schema for founders table
function generateFoundersSchema() {
    return `
-- Portfolio Founders Table
-- Stores founders of portfolio companies for each investor fund

CREATE TABLE IF NOT EXISTS portfolio_founders (
    id VARCHAR(255) PRIMARY KEY,
    company_name VARCHAR(255) NOT NULL,
    company_website VARCHAR(500),
    investor_id VARCHAR(36) REFERENCES investors(id) ON DELETE SET NULL,
    investor_slug VARCHAR(255),
    founder_name VARCHAR(255) NOT NULL,
    founder_title VARCHAR(255),
    founder_linkedin VARCHAR(500),
    confidence VARCHAR(20),
    announced_date DATE,
    amount_raised DECIMAL(15,2),
    enriched_at TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_portfolio_founders_investor ON portfolio_founders(investor_id);
CREATE INDEX IF NOT EXISTS idx_portfolio_founders_company ON portfolio_founders(company_name);
CREATE INDEX IF NOT EXISTS idx_portfolio_founders_linkedin ON portfolio_founders(founder_linkedin);
`;
}

function saveSchema() {
    const schemaPath = path.join(__dirname, '../results/unified_database/portfolio_founders_schema.sql');
    fs.writeFileSync(schemaPath, generateFoundersSchema());
    console.log(`   📄 Schema saved to: ${schemaPath}`);
}

// ============================================================================
// Main
// ============================================================================

async function main() {
    console.log('🚀 Founder Enrichment Script v2');
    console.log('================================\n');
    
    // Parse command line args
    const args = process.argv.slice(2);
    let limit = CONFIG.DEFAULT_LIMIT;
    let sinceDate = CONFIG.DEFAULT_SINCE_DATE;
    let batchSize = CONFIG.BATCH_SIZE;
    
    for (let i = 0; i < args.length; i++) {
        if (args[i] === '--limit' && args[i + 1]) {
            limit = parseInt(args[i + 1]);
        }
        if (args[i] === '--since' && args[i + 1]) {
            sinceDate = args[i + 1];
        }
        if (args[i] === '--batch' && args[i + 1]) {
            batchSize = parseInt(args[i + 1]);
        }
        if (args[i] === '--reset') {
            // Reset progress
            if (fs.existsSync(CONFIG.PROGRESS_PATH)) {
                fs.unlinkSync(CONFIG.PROGRESS_PATH);
                console.log('🔄 Progress reset');
            }
        }
    }
    
    console.log(`📅 Processing investments since: ${sinceDate}`);
    console.log(`📦 Batch size: ${batchSize} companies`);
    if (limit) console.log(`🔢 Limit: ${limit} companies`);
    
    // Load investments
    console.log('\n📂 Loading investments...');
    const investments = JSON.parse(fs.readFileSync(CONFIG.INVESTMENTS_PATH, 'utf8'));
    console.log(`   Total investments: ${investments.length}`);
    
    // Filter by date and get unique companies
    const recentInvestments = investments.filter(inv => 
        inv.announced_date && inv.announced_date >= sinceDate
    );
    console.log(`   Investments since ${sinceDate}: ${recentInvestments.length}`);
    
    // Get unique companies (by name + website)
    const companyMap = new Map();
    for (const inv of recentInvestments) {
        const key = `${inv.company_name}|${inv.company_website || ''}`;
        if (!companyMap.has(key)) {
            companyMap.set(key, inv);
        }
    }
    
    let uniqueCompanies = Array.from(companyMap.values());
    console.log(`   Unique companies: ${uniqueCompanies.length}`);
    
    // Load progress
    const progress = loadProgress();
    const processedSet = new Set(progress.processed);
    
    // Filter out already processed
    uniqueCompanies = uniqueCompanies.filter(c => 
        !processedSet.has(`${c.company_name}|${c.company_website || ''}`)
    );
    console.log(`   Already processed: ${progress.processed.length}`);
    console.log(`   Remaining to process: ${uniqueCompanies.length}`);
    
    // Apply limit
    if (limit) {
        uniqueCompanies = uniqueCompanies.slice(0, limit);
        console.log(`   Processing (with limit): ${uniqueCompanies.length}`);
    }
    
    if (uniqueCompanies.length === 0) {
        console.log('\n✅ All companies already processed!');
        
        // Still save schema
        saveSchema();
        return;
    }
    
    // Load existing founders
    const founders = loadFounders();
    console.log(`   Existing founder records: ${founders.length}`);
    
    // Save schema
    saveSchema();
    
    // Process each company
    console.log('\n🏃 Starting enrichment...');
    console.log('─'.repeat(50));
    
    let processed = 0;
    let foundersFound = 0;
    let linkedInFound = 0;
    let batchCount = 0;
    const startTime = Date.now();
    
    for (const company of uniqueCompanies) {
        try {
            const result = await enrichCompany(company);
            founders.push(result);
            
            const newFounders = result.founders.length;
            const newLinkedIn = result.founders.filter(f => f.linkedin_url).length;
            foundersFound += newFounders;
            linkedInFound += newLinkedIn;
            
            // Update progress
            const key = `${company.company_name}|${company.company_website || ''}`;
            progress.processed.push(key);
            progress.lastRun = new Date().toISOString();
            progress.stats = {
                total: progress.processed.length,
                withFounders: foundersFound,
                withLinkedIn: linkedInFound
            };
            
            processed++;
            batchCount++;
            
            // Save incrementally (every 5 companies)
            if (processed % 5 === 0) {
                saveFounders(founders);
                saveProgress(progress);
                
                const elapsed = (Date.now() - startTime) / 1000;
                const rate = processed / elapsed;
                const remaining = uniqueCompanies.length - processed;
                const eta = remaining / rate;
                
                console.log(`\n📊 Progress: ${processed}/${uniqueCompanies.length} | Founders: ${foundersFound} | LinkedIn: ${linkedInFound} | ETA: ${Math.round(eta/60)}min`);
            }
            
            // Batch pause to avoid rate limiting
            if (batchCount >= batchSize) {
                console.log(`\n⏸️  Batch complete. Pausing ${CONFIG.BATCH_PAUSE/1000}s to avoid rate limits...`);
                await sleep(CONFIG.BATCH_PAUSE);
                batchCount = 0;
            }
            
        } catch (error) {
            console.error(`   ❌ Error processing ${company.company_name}: ${error.message}`);
            
            // Still save progress on error
            saveFounders(founders);
            saveProgress(progress);
        }
    }
    
    // Final save
    saveFounders(founders);
    saveProgress(progress);
    
    const totalTime = (Date.now() - startTime) / 1000;
    
    console.log('\n' + '═'.repeat(50));
    console.log('📊 SUMMARY');
    console.log('═'.repeat(50));
    console.log(`   Companies processed: ${processed}`);
    console.log(`   Founders found: ${foundersFound}`);
    console.log(`   LinkedIn profiles: ${linkedInFound}`);
    console.log(`   Time elapsed: ${Math.round(totalTime/60)} minutes`);
    console.log(`   Rate: ${(processed/totalTime*60).toFixed(1)} companies/min`);
    console.log('');
    console.log('📁 Output files:');
    console.log(`   ${CONFIG.FOUNDERS_PATH}`);
    console.log(`   ${CONFIG.PORTFOLIO_FOUNDERS_PATH}`);
    console.log('\n✅ Done!');
}

main().catch(console.error);

