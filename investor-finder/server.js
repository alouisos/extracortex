const http = require('http');
const fs = require('fs');
const path = require('path');
const { URL } = require('url');
const Stripe = require('stripe');
require('dotenv').config();

// Dynamic imports for ESM modules
let fetch, cheerio, OpenAI;

const PORT = 3000;

// ============================================================================
// Rate Limiting Configuration
// ============================================================================

const RATE_LIMIT = {
    windowMs: 60 * 1000,        // 1 minute window
    maxRequests: 100,           // Max 100 requests per window for general endpoints
    maxSearchRequests: 10,      // Max 10 AI search requests per window (expensive)
    maxAuthRequests: 5,         // Max 5 login/register attempts per window
    blockDurationMs: 5 * 60 * 1000  // Block for 5 minutes if limit exceeded
};

// In-memory rate limit storage: IP -> { requests: [{timestamp}], blocked_until }
const rateLimitStore = new Map();

// Clean up old rate limit entries every 5 minutes
setInterval(() => {
    const now = Date.now();
    for (const [ip, data] of rateLimitStore.entries()) {
        // Remove entries older than the window + block duration
        if (data.blocked_until && data.blocked_until < now) {
            data.blocked_until = null;
        }
        // Clean old requests
        data.requests = data.requests.filter(r => now - r.timestamp < RATE_LIMIT.windowMs);
        data.searchRequests = (data.searchRequests || []).filter(r => now - r.timestamp < RATE_LIMIT.windowMs);
        data.authRequests = (data.authRequests || []).filter(r => now - r.timestamp < RATE_LIMIT.windowMs);
        // Remove empty entries
        if (data.requests.length === 0 && !data.blocked_until) {
            rateLimitStore.delete(ip);
        }
    }
}, 5 * 60 * 1000);

// Get client IP from request (handles proxies)
function getClientIP(req) {
    // Check for forwarded IP (when behind proxy/load balancer)
    const forwarded = req.headers['x-forwarded-for'];
    if (forwarded) {
        return forwarded.split(',')[0].trim();
    }
    const realIP = req.headers['x-real-ip'];
    if (realIP) {
        return realIP.trim();
    }
    // Fallback to socket address
    return req.socket?.remoteAddress || req.connection?.remoteAddress || 'unknown';
}

// Check rate limit for an IP
function checkRateLimit(ip, type = 'general') {
    const now = Date.now();
    
    if (!rateLimitStore.has(ip)) {
        rateLimitStore.set(ip, { 
            requests: [], 
            searchRequests: [],
            authRequests: [],
            blocked_until: null 
        });
    }
    
    const data = rateLimitStore.get(ip);
    
    // Check if IP is blocked
    if (data.blocked_until && data.blocked_until > now) {
        const remainingMs = data.blocked_until - now;
        return { 
            allowed: false, 
            blocked: true,
            retryAfter: Math.ceil(remainingMs / 1000),
            message: `Too many requests. Please try again in ${Math.ceil(remainingMs / 1000)} seconds.`
        };
    }
    
    // Clean old entries
    data.requests = data.requests.filter(r => now - r.timestamp < RATE_LIMIT.windowMs);
    data.searchRequests = (data.searchRequests || []).filter(r => now - r.timestamp < RATE_LIMIT.windowMs);
    data.authRequests = (data.authRequests || []).filter(r => now - r.timestamp < RATE_LIMIT.windowMs);
    
    // Determine which limit to check
    let currentCount, maxLimit, requestArray;
    if (type === 'search') {
        requestArray = data.searchRequests;
        currentCount = requestArray.length;
        maxLimit = RATE_LIMIT.maxSearchRequests;
    } else if (type === 'auth') {
        requestArray = data.authRequests;
        currentCount = requestArray.length;
        maxLimit = RATE_LIMIT.maxAuthRequests;
    } else {
        requestArray = data.requests;
        currentCount = requestArray.length;
        maxLimit = RATE_LIMIT.maxRequests;
    }
    
    // Check if limit exceeded
    if (currentCount >= maxLimit) {
        // Block the IP for the block duration
        data.blocked_until = now + RATE_LIMIT.blockDurationMs;
        console.log(`⚠️ Rate limit exceeded for IP ${ip} (${type}). Blocked until ${new Date(data.blocked_until).toISOString()}`);
        return { 
            allowed: false, 
            blocked: true,
            retryAfter: Math.ceil(RATE_LIMIT.blockDurationMs / 1000),
            message: `Rate limit exceeded. Please try again in ${Math.ceil(RATE_LIMIT.blockDurationMs / 1000)} seconds.`
        };
    }
    
    // Add request to the appropriate counter
    requestArray.push({ timestamp: now });
    
    return { 
        allowed: true, 
        remaining: maxLimit - currentCount - 1,
        limit: maxLimit,
        resetIn: Math.ceil(RATE_LIMIT.windowMs / 1000)
    };
}
const OPENAI_API_KEY = process.env.OPENAI_API_KEY;

// Stripe Configuration
const STRIPE_SECRET_KEY = process.env.STRIPE_SECRET_KEY;
const STRIPE_PUBLISHABLE_KEY = process.env.STRIPE_PUBLISHABLE_KEY;
const STRIPE_PRICE_AMOUNT = 4900; // $49.00 in cents
const stripe = new Stripe(STRIPE_SECRET_KEY);

// Database paths (relative to parent directory)
const DATA_DIR = path.join(__dirname, '..', 'results', 'unified_database');
const INVESTORS_PATH = path.join(DATA_DIR, 'investors.json');
const TEAM_MEMBERS_PATH = path.join(DATA_DIR, 'team_members.json');
const INVESTMENTS_PATH = path.join(DATA_DIR, 'investments.json');
const FOUNDERS_PATH = path.join(DATA_DIR, 'founders.json');
const ENRICHED_PATH = path.join(DATA_DIR, 'enriched_investors.json');
const USERS_PATH = path.join(__dirname, 'users.json');

// In-memory database
let investors = [];
let teamMembers = [];
let investments = [];
let founders = [];
let enrichedInvestors = {}; // investor_id -> enrichment data
let foundersByCompany = new Map(); // company_name -> founders[]
let users = []; // User accounts
let sessions = new Map(); // sessionToken -> user
let openai = null;

// ============================================================================
// User Authentication System
// ============================================================================

const crypto = require('crypto');

function hashPassword(password) {
    return crypto.createHash('sha256').update(password).digest('hex');
}

function generateSessionToken() {
    return crypto.randomBytes(32).toString('hex');
}

function loadUsers() {
    try {
        if (fs.existsSync(USERS_PATH)) {
            users = JSON.parse(fs.readFileSync(USERS_PATH, 'utf-8'));
            console.log(`   ✅ Loaded ${users.length} users`);
        } else {
            users = [];
            saveUsers();
            console.log('   ✅ Created new users database');
        }
    } catch (err) {
        console.error('   ❌ Error loading users:', err.message);
        users = [];
    }
}

function saveUsers() {
    fs.writeFileSync(USERS_PATH, JSON.stringify(users, null, 2));
}

function findUserByEmail(email) {
    return users.find(u => u.email.toLowerCase() === email.toLowerCase());
}

function findUserById(id) {
    return users.find(u => u.id === id);
}

function findUserBySession(sessionToken) {
    return sessions.get(sessionToken);
}

// Add a search to user's history
function addSearchToHistory(userId, searchData) {
    const user = findUserById(userId);
    if (!user) return;
    
    if (!user.search_history) {
        user.search_history = [];
    }
    
    // Add new search at the beginning
    user.search_history.unshift({
        id: crypto.randomUUID(),
        query: searchData.query,
        company_name: searchData.company_name,
        matches_count: searchData.matches_count,
        type: searchData.type || 'website_search', // 'website_search' or 'fund_search'
        searched_at: new Date().toISOString()
    });
    
    // Keep only last 20 searches
    if (user.search_history.length > 20) {
        user.search_history = user.search_history.slice(0, 20);
    }
    
    saveUsers();
}

function getSessionFromCookie(req) {
    const cookies = req.headers.cookie;
    if (!cookies) return null;
    
    const match = cookies.match(/session=([^;]+)/);
    return match ? match[1] : null;
}

function setSessionCookie(res, sessionToken) {
    // Set cookie that expires in 7 days
    const expires = new Date(Date.now() + 7 * 24 * 60 * 60 * 1000).toUTCString();
    res.setHeader('Set-Cookie', `session=${sessionToken}; Path=/; HttpOnly; Expires=${expires}`);
}

function clearSessionCookie(res) {
    res.setHeader('Set-Cookie', 'session=; Path=/; HttpOnly; Expires=Thu, 01 Jan 1970 00:00:00 GMT');
}

// MIME types for static files
const MIME_TYPES = {
    '.html': 'text/html',
    '.css': 'text/css',
    '.js': 'application/javascript',
    '.json': 'application/json',
    '.png': 'image/png',
    '.jpg': 'image/jpeg',
    '.ico': 'image/x-icon'
};

// Initialize dynamic imports and database
async function initialize() {
    console.log('🚀 Initializing Investor Match...\n');
    
    // Load ESM modules
    console.log('📦 Loading modules...');
    const fetchModule = await import('node-fetch');
    fetch = fetchModule.default;
    
    const cheerioModule = await import('cheerio');
    cheerio = cheerioModule;
    
    const OpenAIModule = await import('openai');
    OpenAI = OpenAIModule.default;
    
    // Initialize OpenAI client
    openai = new OpenAI({ apiKey: OPENAI_API_KEY });
    console.log('✅ OpenAI client initialized\n');
    
    // Load database
    console.log('📂 Loading database...');
    
    try {
        const investorsData = fs.readFileSync(INVESTORS_PATH, 'utf-8');
        investors = JSON.parse(investorsData);
        console.log(`   ✅ Loaded ${investors.length.toLocaleString()} investors`);
    } catch (err) {
        console.error('   ❌ Error loading investors:', err.message);
    }
    
    try {
        const teamData = fs.readFileSync(TEAM_MEMBERS_PATH, 'utf-8');
        teamMembers = JSON.parse(teamData);
        console.log(`   ✅ Loaded ${teamMembers.length.toLocaleString()} team members`);
    } catch (err) {
        console.error('   ❌ Error loading team members:', err.message);
    }
    
    try {
        const investmentsData = fs.readFileSync(INVESTMENTS_PATH, 'utf-8');
        investments = JSON.parse(investmentsData);
        console.log(`   ✅ Loaded ${investments.length.toLocaleString()} investments`);
    } catch (err) {
        console.error('   ❌ Error loading investments:', err.message);
    }
    
    // Load founders data
    try {
        if (fs.existsSync(FOUNDERS_PATH)) {
            const foundersData = fs.readFileSync(FOUNDERS_PATH, 'utf-8');
            founders = JSON.parse(foundersData);
            console.log(`   ✅ Loaded ${founders.length.toLocaleString()} founder records`);
            
            // Build company -> founders index
            for (const record of founders) {
                const key = record.company_name?.toLowerCase();
                if (key && record.founders && record.founders.length > 0) {
                    foundersByCompany.set(key, record.founders);
                }
            }
            console.log(`   ✅ Indexed founders for ${foundersByCompany.size.toLocaleString()} companies`);
        } else {
            console.log('   ⚠️ No founders.json found (run founder-enrichment.js to generate)');
        }
    } catch (err) {
        console.error('   ❌ Error loading founders:', err.message);
    }
    
    // Load enriched investor data (approach guides)
    try {
        if (fs.existsSync(ENRICHED_PATH)) {
            enrichedInvestors = JSON.parse(fs.readFileSync(ENRICHED_PATH, 'utf-8'));
            const count = Object.keys(enrichedInvestors).length;
            console.log(`   ✅ Loaded ${count.toLocaleString()} enriched investor profiles`);
        } else {
            console.log('   ⚠️ No enriched_investors.json found (run investor-enrichment.js to generate)');
        }
    } catch (err) {
        console.error('   ❌ Error loading enriched investors:', err.message);
    }
    
    // Load users
    loadUsers();
    
    // Build lookup indexes for faster queries
    buildIndexes();
    
    console.log('\n✅ Database loaded successfully!\n');
}

// Build indexes for faster lookups
const investorById = new Map();
const teamByInvestorId = new Map();
const investmentsByInvestorId = new Map();

function buildIndexes() {
    console.log('🔧 Building indexes...');
    
    // Index investors by ID
    investors.forEach(inv => {
        investorById.set(inv.id, inv);
    });
    
    // Index team members by investor ID
    teamMembers.forEach(tm => {
        if (!teamByInvestorId.has(tm.investor_id)) {
            teamByInvestorId.set(tm.investor_id, []);
        }
        teamByInvestorId.get(tm.investor_id).push(tm);
    });
    
    // Index investments by investor ID
    investments.forEach(inv => {
        if (!investmentsByInvestorId.has(inv.investor_id)) {
            investmentsByInvestorId.set(inv.investor_id, []);
        }
        investmentsByInvestorId.get(inv.investor_id).push(inv);
    });
    
    console.log('   ✅ Indexes built');
}

// Scrape website content
async function scrapeWebsite(url) {
    console.log(`\n🌐 Scraping website: ${url}`);
    
    try {
        // Normalize URL
        if (!url.startsWith('http://') && !url.startsWith('https://')) {
            url = 'https://' + url;
        }
        
        const response = await fetch(url, {
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5'
            },
            timeout: 15000
        });
        
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }
        
        const html = await response.text();
        const $ = cheerio.load(html);
        
        // Remove scripts, styles, and other non-content elements
        $('script, style, nav, footer, header, iframe, noscript').remove();
        
        // Extract relevant content
        const title = $('title').text().trim();
        const metaDescription = $('meta[name="description"]').attr('content') || '';
        const metaKeywords = $('meta[name="keywords"]').attr('content') || '';
        const ogTitle = $('meta[property="og:title"]').attr('content') || '';
        const ogDescription = $('meta[property="og:description"]').attr('content') || '';
        
        // Get main content text
        const bodyText = $('body').text()
            .replace(/\s+/g, ' ')
            .trim()
            .substring(0, 8000); // Limit to avoid token limits
        
        // Get headings
        const headings = [];
        $('h1, h2, h3').each((i, el) => {
            const text = $(el).text().trim();
            if (text && text.length < 200) {
                headings.push(text);
            }
        });
        
        const domain = new URL(url).hostname.replace('www.', '');
        
        console.log(`   ✅ Scraped successfully (${bodyText.length} chars)`);
        
        return {
            url,
            domain,
            title,
            metaDescription,
            metaKeywords,
            ogTitle,
            ogDescription,
            headings: headings.slice(0, 20),
            bodyText
        };
    } catch (error) {
        console.error(`   ❌ Scraping error: ${error.message}`);
        throw new Error(`Failed to scrape website: ${error.message}`);
    }
}

// Analyze company with OpenAI
async function analyzeCompany(scrapedData) {
    console.log('\n🤖 Analyzing with AI...');
    
    const prompt = `Analyze this company website and extract structured information for investor matching.

Website: ${scrapedData.url}
Domain: ${scrapedData.domain}
Title: ${scrapedData.title}
Meta Description: ${scrapedData.metaDescription}
OG Description: ${scrapedData.ogDescription}
Keywords: ${scrapedData.metaKeywords}

Headings:
${scrapedData.headings.join('\n')}

Content:
${scrapedData.bodyText.substring(0, 6000)}

Based on this information, provide a JSON response with the following structure:
{
    "company_name": "The company name",
    "description": "A 2-3 sentence description of what the company does",
    "industry": "Primary industry (e.g., 'SaaS', 'Fintech', 'Healthcare', 'E-commerce', 'AI/ML', 'Consumer', 'Enterprise Software')",
    "business_model": "B2B, B2C, B2B2C, Marketplace, or Platform",
    "target_market": "Who are their customers",
    "geography": "Target geography (e.g., 'United States', 'Global', 'Europe')",
    "estimated_stage": "Pre-Seed, Seed, Series A, Series B, Growth, or Unknown",
    "tags": ["array", "of", "relevant", "tags", "for", "investor", "matching"],
    "focus_areas": ["primary", "focus", "areas"],
    "confidence": 0.0 to 1.0
}

The tags should match common VC investment themes like: AI, SaaS, Fintech, Healthcare, B2B, B2C, Enterprise, Consumer, Marketplace, Mobile, Cloud, DevOps, Cybersecurity, Data Analytics, Machine Learning, E-commerce, EdTech, PropTech, InsurTech, HealthTech, FoodTech, CleanTech, Biotech, Hardware, IoT, AR/VR, Gaming, Social, Media, Advertising, Marketing Technology, HR Tech, Legal Tech, Supply Chain, Logistics, Transportation, Real Estate, Financial Services, Payments, Lending, Insurance, Wealth Management, etc.

Return ONLY valid JSON, no markdown or explanation.`;

    try {
        const response = await openai.chat.completions.create({
            model: 'gpt-4o',
            messages: [
                {
                    role: 'system',
                    content: 'You are an expert venture capital analyst who analyzes companies and matches them with relevant investors. Always respond with valid JSON only.'
                },
                {
                    role: 'user',
                    content: prompt
                }
            ],
            temperature: 0.3,
            max_tokens: 1000
        });
        
        const content = response.choices[0].message.content.trim();
        
        // Parse JSON (handle potential markdown code blocks)
        let jsonStr = content;
        if (content.startsWith('```')) {
            jsonStr = content.replace(/```json?\n?/g, '').replace(/```/g, '').trim();
        }
        
        const analysis = JSON.parse(jsonStr);
        console.log(`   ✅ Analysis complete: ${analysis.company_name} (${analysis.industry})`);
        
        return {
            ...analysis,
            source_url: scrapedData.url,
            source_domain: scrapedData.domain
        };
    } catch (error) {
        console.error(`   ❌ AI analysis error: ${error.message}`);
        throw new Error(`Failed to analyze company: ${error.message}`);
    }
}

// ============================================================================
// IMPROVED MATCHING ALGORITHM v2
// - A) Focus weighted higher than tags (no double counting)
// - B) Portfolio similarity matching
// - C) Transparent scoring breakdown
// ============================================================================

// Helper to ensure value is an array
function toArray(val) {
    if (!val) return [];
    if (Array.isArray(val)) return val;
    if (typeof val === 'string') return [val];
    return [];
}

// Normalize text for comparison
function normalize(text) {
    if (!text) return '';
    return String(text).toLowerCase().trim();
}

// Normalize URL for comparison
function normalizeUrl(url) {
    if (!url) return '';
    return url.toLowerCase()
        .replace(/^https?:\/\//, '')
        .replace(/^www\./, '')
        .replace(/\/$/, '')
        .trim();
}

// Semantic category mapping - maps various terms to canonical categories
const CATEGORY_MAP = {
    // AI & ML
    'ai': 'ai', 'artificial intelligence': 'ai', 'machine learning': 'ai', 'ml': 'ai', 
    'deep learning': 'ai', 'generative ai': 'ai', 'llm': 'ai',
    
    // SaaS & Software
    'saas': 'saas', 'software': 'saas', 'software as a service': 'saas', 'cloud': 'saas',
    'enterprise software': 'saas', 'b2b software': 'saas',
    
    // Fintech
    'fintech': 'fintech', 'financial technology': 'fintech', 'payments': 'fintech',
    'banking': 'fintech', 'financial services': 'fintech', 'lending': 'fintech',
    'wealth management': 'fintech', 'insurtech': 'fintech', 'insurance': 'fintech',
    'b2b payments': 'fintech',
    
    // Healthcare
    'healthcare': 'healthcare', 'health care': 'healthcare', 'healthtech': 'healthcare',
    'health tech': 'healthcare', 'digital health': 'healthcare', 'medtech': 'healthcare',
    'biotech': 'healthcare', 'biotechnology': 'healthcare', 'life sciences': 'healthcare',
    
    // E-commerce & Marketplaces
    'ecommerce': 'ecommerce', 'e-commerce': 'ecommerce', 'commerce': 'ecommerce',
    'marketplace': 'ecommerce', 'marketplaces': 'ecommerce', 'retail': 'ecommerce',
    'd2c': 'ecommerce', 'direct to consumer': 'ecommerce',
    
    // B2B & Enterprise
    'b2b': 'b2b', 'enterprise': 'b2b', 'b2b & enterprise': 'b2b',
    'business to business': 'b2b',
    
    // Consumer
    'b2c': 'consumer', 'consumer': 'consumer', 'b2b2c': 'consumer',
    'consumer services': 'consumer',
    
    // Developer Tools & Infrastructure
    'developer tools': 'devtools', 'devtools': 'devtools', 'dev tools': 'devtools',
    'infrastructure': 'devtools', 'api': 'devtools', 'developer': 'devtools',
    'data infrastructure': 'devtools', 'cloud infrastructure': 'devtools',
    
    // Security
    'cybersecurity': 'security', 'security': 'security', 'privacy': 'security',
    'privacy and security': 'security', 'infosec': 'security',
    
    // Data & Analytics
    'analytics': 'data', 'data': 'data', 'data analytics': 'data', 'big data': 'data',
    
    // Climate & Sustainability
    'climate': 'climate', 'climate tech': 'climate', 'cleantech': 'climate',
    'sustainability': 'climate', 'energy': 'climate', 'clean energy': 'climate',
    
    // Real Estate
    'proptech': 'realestate', 'real estate': 'realestate', 'property': 'realestate',
    
    // Education
    'edtech': 'education', 'education': 'education', 'ed tech': 'education',
    
    // Logistics & Supply Chain
    'logistics': 'logistics', 'supply chain': 'logistics', 'transportation': 'logistics',
    'supply chain management': 'logistics',
    
    // HR & Future of Work
    'hr': 'hr', 'hr tech': 'hr', 'human resources': 'hr', 'recruiting': 'hr',
    'future of work': 'hr',
    
    // Marketing
    'marketing': 'marketing', 'marketing technology': 'marketing', 'martech': 'marketing',
    'advertising': 'marketing', 'adtech': 'marketing',
    
    // Other verticals
    'legal tech': 'legal', 'legaltech': 'legal',
    'foodtech': 'food', 'food': 'food', 'agriculture': 'food', 'agtech': 'food',
    'gaming': 'gaming', 'games': 'gaming', 'esports': 'gaming',
    'media': 'media', 'content': 'media', 'media and entertainment': 'media',
    'robotics': 'robotics', 'automation': 'robotics',
    'iot': 'iot', 'internet of things': 'iot', 'hardware': 'iot',
    'blockchain': 'crypto', 'crypto': 'crypto', 'web3': 'crypto',
    'mobile': 'mobile', 'apps': 'mobile', 'applications': 'mobile',
};

// Get canonical category for a term
function getCategory(term) {
    const normalized = normalize(term);
    // Direct lookup
    if (CATEGORY_MAP[normalized]) return CATEGORY_MAP[normalized];
    // Partial match
    for (const [key, category] of Object.entries(CATEGORY_MAP)) {
        if (normalized.includes(key) || key.includes(normalized)) {
            return category;
        }
    }
    return normalized; // Return as-is if no mapping
}

// Extract categories from a list of terms
function extractCategories(terms) {
    const categories = new Set();
    for (const term of terms) {
        const cat = getCategory(term);
        if (cat) categories.add(cat);
    }
    return categories;
}

// Match investors based on company analysis
function matchInvestors(companyAnalysis, limit = 50) {
    console.log('\n🔍 Matching investors...');
    
    // ========================================================================
    // STEP 1: Extract and categorize company attributes
    // ========================================================================
    const companyName = normalize(companyAnalysis.company_name || '');
    const companyIndustry = normalize(companyAnalysis.industry || '');
    const companyModel = normalize(companyAnalysis.business_model || '');
    const companyStage = normalize(companyAnalysis.estimated_stage || '');
    const companyGeo = normalize(companyAnalysis.geography || '');
    const companyTags = toArray(companyAnalysis.tags).map(normalize);
    const companyFocusAreas = toArray(companyAnalysis.focus_areas).map(normalize);
    
    // Get canonical categories for the company
    const companyCategories = extractCategories([
        companyIndustry,
        companyModel,
        ...companyTags,
        ...companyFocusAreas
    ]);
    
    console.log(`   Company: ${companyAnalysis.company_name}`);
    console.log(`   Industry: ${companyIndustry}`);
    console.log(`   Categories: ${[...companyCategories].join(', ')}`);
    console.log(`   Stage: ${companyStage}`);
    
    // ========================================================================
    // STEP 2: Score each investor
    // ========================================================================
    const scoredInvestors = investors.map(investor => {
        const scores = {
            focusMatch: 0,      // Max 40 points - PRIMARY focus alignment
            tagMatch: 0,        // Max 20 points - Secondary tag alignment  
            portfolioMatch: 0,  // Max 25 points - Similar portfolio companies
            stageMatch: 0,      // Max 15 points - Stage alignment
            hasContacts: 0,     // Max 10 points - Practical utility
        };
        const matchReasons = [];
        
        // Get investor's focus and tags as categories
        const investorFocus = toArray(investor.focus).map(normalize);
        const investorTags = toArray(investor.tags).map(normalize);
        const investorStages = toArray(investor.stages).map(normalize);
        
        const investorFocusCategories = extractCategories(investorFocus);
        const investorTagCategories = extractCategories(investorTags);
        
        // --------------------------------------------------------------------
        // A) FOCUS MATCH (40 points max) - Investor's primary investment focus
        // This is what the investor ACTIVELY seeks
        // --------------------------------------------------------------------
        const focusOverlap = [...companyCategories].filter(c => investorFocusCategories.has(c));
        if (focusOverlap.length > 0) {
            // First match = 25 pts, each additional = 5 pts (max 40)
            scores.focusMatch = Math.min(40, 25 + (focusOverlap.length - 1) * 5);
            matchReasons.push(`Focus: ${focusOverlap.join(', ')}`);
        }
        
        // --------------------------------------------------------------------
        // TAG MATCH (20 points max) - Only count tags NOT already in focus
        // Avoid double-counting
        // --------------------------------------------------------------------
        const tagOnlyCategories = [...investorTagCategories].filter(c => !investorFocusCategories.has(c));
        const tagOverlap = [...companyCategories].filter(c => tagOnlyCategories.includes(c));
        if (tagOverlap.length > 0) {
            scores.tagMatch = Math.min(20, tagOverlap.length * 7);
            matchReasons.push(`Tags: ${tagOverlap.join(', ')}`);
        }
        
        // --------------------------------------------------------------------
        // B) PORTFOLIO SIMILARITY (25 points max) - Have they invested in similar companies?
        // --------------------------------------------------------------------
        const portfolio = investmentsByInvestorId.get(investor.id) || [];
        let portfolioMatches = [];
        
        if (portfolio.length > 0) {
            for (const investment of portfolio) {
                const portfolioCompanyName = normalize(investment.company_name || '');
                
                // Check if portfolio company is in a similar space
                // We can infer from company name patterns or check against known categories
                let similarity = 0;
                
                // Check for keyword matches in portfolio company names
                for (const category of companyCategories) {
                    if (portfolioCompanyName.includes(category) || 
                        (investment.company_website && normalize(investment.company_website).includes(category))) {
                        similarity += 1;
                    }
                }
                
                // Check for industry-specific patterns in company names
                const industryPatterns = {
                    'fintech': ['pay', 'bank', 'fin', 'money', 'credit', 'loan', 'wallet'],
                    'healthcare': ['health', 'med', 'care', 'bio', 'pharma', 'clinic', 'doctor'],
                    'ai': ['ai', 'ml', 'intelligence', 'neural', 'cognitive', 'smart'],
                    'saas': ['cloud', 'platform', 'software', 'app'],
                    'ecommerce': ['shop', 'store', 'cart', 'commerce', 'market'],
                    'security': ['secure', 'cyber', 'protect', 'guard', 'shield'],
                    'data': ['data', 'analytics', 'insight', 'metric'],
                };
                
                for (const category of companyCategories) {
                    const patterns = industryPatterns[category] || [];
                    for (const pattern of patterns) {
                        if (portfolioCompanyName.includes(pattern)) {
                            similarity += 0.5;
                            break;
                        }
                    }
                }
                
                if (similarity > 0) {
                    portfolioMatches.push({
                        company: investment.company_name,
                        score: similarity
                    });
                }
            }
            
            // Score based on number and quality of portfolio matches
            if (portfolioMatches.length > 0) {
                // Sort by similarity score
                portfolioMatches.sort((a, b) => b.score - a.score);
                const topMatches = portfolioMatches.slice(0, 5);
                const avgScore = topMatches.reduce((sum, m) => sum + m.score, 0) / topMatches.length;
                
                // 5 points per similar company, max 25
                scores.portfolioMatch = Math.min(25, topMatches.length * 5);
                
                if (topMatches.length > 0) {
                    matchReasons.push(`Portfolio: ${topMatches.slice(0, 3).map(m => m.company).join(', ')}`);
                }
            }
        }
        
        // --------------------------------------------------------------------
        // STAGE MATCH (15 points)
        // --------------------------------------------------------------------
        if (companyStage && investorStages.length > 0) {
            const stageMapping = {
                'pre-seed': ['pre-seed', 'preseed', 'angel', 'accelerator'],
                'seed': ['seed', 'pre-seed'],
                'series a': ['series a', 'seed'],
                'series b': ['series b', 'series a', 'growth'],
                'series c': ['series c', 'series b', 'growth'],
                'growth': ['growth', 'series b', 'series c', 'late stage'],
            };
            
            const relevantStages = stageMapping[companyStage] || [companyStage];
            const stageMatch = investorStages.some(s => 
                relevantStages.some(rs => s.includes(rs) || rs.includes(s))
            );
            
            if (stageMatch) {
                scores.stageMatch = 15;
                matchReasons.push(`Stage: ${companyStage}`);
            }
        }
        
        // --------------------------------------------------------------------
        // HAS CONTACTS (10 points) - Practical utility bonus
        // --------------------------------------------------------------------
        const team = teamByInvestorId.get(investor.id) || [];
        const teamWithEmails = team.filter(t => 
            (t.emails && t.emails.length > 0) || 
            (t.work_emails && t.work_emails.length > 0)
        );
        
        if (teamWithEmails.length > 0) {
            scores.hasContacts = 10;
        }
        
        // --------------------------------------------------------------------
        // C) CALCULATE TOTAL SCORE (Transparent breakdown)
        // --------------------------------------------------------------------
        const totalScore = scores.focusMatch + scores.tagMatch + scores.portfolioMatch + 
                          scores.stageMatch + scores.hasContacts;
        
        return {
            investor,
            score: totalScore,
            scores, // Detailed breakdown
            matchReasons,
            teamMembers: team,
            teamWithEmails: teamWithEmails.length,
            portfolioCount: portfolio.length,
            portfolioMatches: portfolioMatches.slice(0, 5),
            recentInvestments: portfolio
                .sort((a, b) => new Date(b.announced_date || 0) - new Date(a.announced_date || 0))
                .slice(0, 5)
        };
    });
    
    // ========================================================================
    // STEP 3: Filter and sort
    // ========================================================================
    const matches = scoredInvestors
        .filter(m => m.score >= 25) // Must have meaningful match (focus OR portfolio)
        .sort((a, b) => {
            // Primary: total score
            if (b.score !== a.score) return b.score - a.score;
            // Secondary: prefer investors with contacts
            return b.teamWithEmails - a.teamWithEmails;
        })
        .slice(0, limit);
    
    console.log(`   ✅ Found ${matches.length} matching investors`);
    
    return matches;
}

// Format investor results for API response
function formatResults(matches, companyAnalysis) {
    return {
        company: companyAnalysis,
        total_matches: matches.length,
        investors: matches.map(m => ({
            id: m.investor.id,
            name: m.investor.name,
            slug: m.investor.slug,
            website: m.investor.website,
            description: m.investor.description?.substring(0, 300),
            location: [m.investor.city, m.investor.region, m.investor.country].filter(Boolean).join(', '),
            check_size: {
                min: m.investor.check_size_min,
                max: m.investor.check_size_max,
                sweet_spot: m.investor.sweet_spot
            },
            fund_size: m.investor.fund_size,
            stages: m.investor.stages || [],
            focus: m.investor.focus || [],
            tags: m.investor.tags || [],
            thesis: m.investor.thesis?.substring(0, 300),
            verified: m.investor.verified,
            profile_url: m.investor.profile_url,
            linkedin_url: m.investor.linkedin_url,
            twitter_url: m.investor.twitter_url,
            match_score: m.score,
            score_breakdown: m.scores, // Detailed scoring transparency
            match_reasons: m.matchReasons,
            portfolio_count: m.portfolioCount,
            portfolio_matches: m.portfolioMatches || [],
            recent_investments: m.recentInvestments.map(inv => {
                // Look up founders for this company
                const companyFounders = foundersByCompany.get(inv.company_name?.toLowerCase()) || [];
                return {
                    company: inv.company_name,
                    website: inv.company_website,
                    amount: inv.amount_raised,
                    date: inv.announced_date,
                    founders: companyFounders.map(f => ({
                        name: f.name,
                        title: f.title,
                        linkedin_url: f.linkedin_url
                    }))
                };
            }),
            team: m.teamMembers.map(tm => ({
                name: tm.name,
                title: tm.title,
                emails: tm.emails || [],
                work_emails: tm.work_emails || [],
                personal_emails: tm.personal_emails || [],
                phones: tm.phones || [],
                linkedin_url: tm.linkedin_url,
                location: tm.location
            })).filter(tm => 
                (tm.emails && tm.emails.length > 0) || 
                (tm.work_emails && tm.work_emails.length > 0) ||
                tm.linkedin_url
            ),
            // Founder Approach Guide (enrichment data)
            approach_guide: enrichedInvestors[m.investor.id] || null
        }))
    };
}

// ============================================================================
// FUND SEARCH FUNCTIONS
// ============================================================================

// Helper to ensure array
function toArray(val) {
    if (!val) return [];
    if (Array.isArray(val)) return val;
    if (typeof val === 'string') return val.split(',').map(s => s.trim()).filter(Boolean);
    return [];
}

// Get available filter options from the database
function getFilterOptions() {
    const stages = new Set();
    const focusCounts = new Map(); // Track focus area frequency
    const tags = new Set();
    const locations = new Set();
    const usCityCounts = new Map(); // Track US city frequency
    
    investors.forEach(inv => {
        toArray(inv.stages).forEach(s => stages.add(s));
        toArray(inv.focus).forEach(f => {
            focusCounts.set(f, (focusCounts.get(f) || 0) + 1);
        });
        toArray(inv.tags).slice(0, 10).forEach(t => tags.add(t));
        if (inv.country) locations.add(inv.country);
        
        // Collect US cities
        const isUS = inv.country === 'United States' || inv.country === 'USA' || inv.country === 'US' || 
                     (!inv.country && inv.city && ['San Francisco', 'New York', 'Los Angeles', 'Boston', 'Chicago', 'Austin', 'Seattle', 'Miami', 'Denver', 'Atlanta', 'Palo Alto', 'Menlo Park', 'Mountain View'].includes(inv.city));
        if (isUS && inv.city) {
            usCityCounts.set(inv.city, (usCityCounts.get(inv.city) || 0) + 1);
        }
    });
    
    // Sort focuses by frequency and take top 30
    const topFocuses = Array.from(focusCounts.entries())
        .sort((a, b) => b[1] - a[1])
        .slice(0, 30)
        .map(([focus, count]) => ({ name: focus, count }));
    
    // Sort US cities by frequency and take top 30
    const topUSCities = Array.from(usCityCounts.entries())
        .sort((a, b) => b[1] - a[1])
        .slice(0, 30)
        .map(([city, count]) => ({ name: city, count }));
    
    // Ensure United States is in the locations list and put it first
    locations.add('United States');
    const sortedLocations = Array.from(locations).sort();
    // Move United States to the front
    const usIndex = sortedLocations.indexOf('United States');
    if (usIndex > 0) {
        sortedLocations.splice(usIndex, 1);
        sortedLocations.unshift('United States');
    }
    
    return {
        stages: Array.from(stages).sort(),
        focuses: topFocuses,
        tags: Array.from(tags).sort().slice(0, 100), // Top 100 tags
        locations: sortedLocations,
        us_cities: topUSCities,
        check_size_ranges: [
            { label: 'Under $100K', min: 0, max: 100000 },
            { label: '$100K - $500K', min: 100000, max: 500000 },
            { label: '$500K - $1M', min: 500000, max: 1000000 },
            { label: '$1M - $5M', min: 1000000, max: 5000000 },
            { label: '$5M - $10M', min: 5000000, max: 10000000 },
            { label: '$10M+', min: 10000000, max: null }
        ],
        recently_active_options: [
            { label: 'Last 30 days', days: 30 },
            { label: 'Last 90 days', days: 90 },
            { label: 'Last 6 months', days: 180 },
            { label: 'Last year', days: 365 }
        ]
    };
}

// Parse natural language query with AI
async function parseNaturalLanguageQuery(query) {
    if (!openai) {
        console.log('OpenAI not initialized, skipping natural language parsing');
        return null;
    }
    
    try {
        console.log('Parsing natural language query:', query);
        const response = await openai.chat.completions.create({
            model: 'gpt-4o',
            messages: [
                {
                    role: 'system',
                    content: `You are a search query parser for a VC/investor database. Extract structured filters from natural language queries.

Return a JSON object with these optional fields:
- name: string (fund name to search for)
- stages: array of strings (Pre-Seed, Seed, Series A, Series B, Series C, Growth)
- focuses: array of strings (industry focus areas like SaaS, Fintech, Healthcare, AI, etc.)
- tags: array of strings (specific keywords)
- locations: array of strings (countries or cities)
- check_size_min: number (minimum check size in USD)
- check_size_max: number (maximum check size in USD)
- has_contacts: boolean (if they want funds with contact info)

Examples:
"AI focused seed stage VCs in San Francisco" → {"focuses": ["AI"], "stages": ["Seed"], "locations": ["San Francisco", "United States"]}
"Fintech investors who write $1-5M checks" → {"focuses": ["Fintech"], "check_size_min": 1000000, "check_size_max": 5000000}
"Series A healthcare funds" → {"stages": ["Series A"], "focuses": ["Healthcare"]}
"Andreessen Horowitz" → {"name": "Andreessen Horowitz"}

Only include fields that are clearly mentioned. Return valid JSON only.`
                },
                {
                    role: 'user',
                    content: query
                }
            ],
            temperature: 0.1,
            max_tokens: 500
        });
        
        const content = response.choices[0].message.content;
        console.log('AI parsed response:', content);
        // Extract JSON from response
        const jsonMatch = content.match(/\{[\s\S]*\}/);
        if (jsonMatch) {
            const parsed = JSON.parse(jsonMatch[0]);
            console.log('Parsed filters:', parsed);
            return parsed;
        }
        return null;
    } catch (error) {
        console.error('Error parsing natural language query:', error.message || error);
        // Return null to allow search to continue without AI parsing
        return null;
    }
}

// Search funds with filters
async function searchFunds(filters) {
    const {
        query,           // Natural language query
        name,            // Fund name search
        stages,          // Array of stages
        focuses,         // Array of focus areas
        tags,            // Array of tags
        locations,       // Array of locations
        us_city,         // US city filter
        check_size_min,  // Minimum check size
        check_size_max,  // Maximum check size
        has_contacts,    // Only funds with contacts
        verified_only,   // Only verified funds
        recently_active_days, // Only funds with investments in last N days
        page = 1,
        limit = 50
    } = filters;
    
    // Check if query looks like a direct fund name or person name search (no filter keywords)
    const filterKeywords = ['seed', 'series', 'pre-seed', 'growth', 'ai', 'saas', 'fintech', 'healthcare', 
        'crypto', 'b2b', 'consumer', 'in ', 'at ', 'from ', 'with ', 'focused', 'stage', 'investors', 
        'funds', 'vcs', 'venture', 'capital', '$', 'million', 'checks'];
    
    const queryLower = (query || '').toLowerCase().trim();
    const looksLikeDirectSearch = queryLower && 
        queryLower.length > 2 && 
        !filterKeywords.some(kw => queryLower.includes(kw));
    
    // First, try to find team members (individual investors) by name
    if (looksLikeDirectSearch) {
        const matchingTeamMembers = teamMembers.filter(tm => {
            const tmName = (tm.name || '').toLowerCase();
            return tmName.includes(queryLower) || 
                   queryLower.includes(tmName) ||
                   tmName.split(/\s+/).some(word => word.toLowerCase().startsWith(queryLower)) ||
                   queryLower.split(/\s+/).every(qWord => tmName.includes(qWord));
        });
        
        if (matchingTeamMembers.length > 0 && matchingTeamMembers.length <= 50) {
            // Found matching team members - return their funds
            console.log(`Person search for "${query}" found ${matchingTeamMembers.length} team members`);
            
            // Get unique investor IDs for these team members
            const matchingInvestorIds = new Set(matchingTeamMembers.map(tm => tm.investor_id));
            const matchingFunds = investors.filter(inv => matchingInvestorIds.has(inv.id));
            
            // Sort funds by how many matching team members they have
            const fundMatchCounts = new Map();
            matchingTeamMembers.forEach(tm => {
                fundMatchCounts.set(tm.investor_id, (fundMatchCounts.get(tm.investor_id) || 0) + 1);
            });
            
            matchingFunds.sort((a, b) => {
                const aCount = fundMatchCounts.get(a.id) || 0;
                const bCount = fundMatchCounts.get(b.id) || 0;
                return bCount - aCount;
            });
            
            const total = matchingFunds.length;
            const startIndex = (page - 1) * limit;
            const paginatedResults = matchingFunds.slice(startIndex, startIndex + limit);
            
            const formattedResults = paginatedResults.map(inv => {
                const team = teamByInvestorId.get(inv.id) || [];
                const teamWithContacts = team.filter(tm => 
                    (tm.emails && tm.emails.length > 0) || 
                    (tm.work_emails && tm.work_emails.length > 0) ||
                    tm.linkedin_url
                );
                const portfolio = investmentsByInvestorId.get(inv.id) || [];
                
                // Highlight matching team members
                const matchingMembers = team.filter(tm => {
                    const tmName = (tm.name || '').toLowerCase();
                    return tmName.includes(queryLower) || queryLower.includes(tmName);
                }).map(tm => tm.name);
                
                return {
                    id: inv.id,
                    name: inv.name,
                    slug: inv.slug,
                    website: inv.website,
                    description: inv.description?.substring(0, 200),
                    location: [inv.city, inv.region, inv.country].filter(Boolean).join(', '),
                    check_size: {
                        min: inv.check_size_min,
                        max: inv.check_size_max,
                        sweet_spot: inv.sweet_spot
                    },
                    stages: toArray(inv.stages),
                    focus: toArray(inv.focus),
                    tags: toArray(inv.tags).slice(0, 8),
                    verified: inv.verified,
                    contacts_count: teamWithContacts.length,
                    portfolio_count: portfolio.length,
                    matching_team_members: matchingMembers
                };
            });
            
            return {
                results: formattedResults,
                total,
                page,
                limit,
                total_pages: Math.ceil(total / limit),
                parsed_query: { person_name: query },
                person_search: true
            };
        }
    }
    
    // Try direct fund name search if it looks like a fund name
    if (looksLikeDirectSearch) {
        const directMatches = investors.filter(inv => {
            const invName = (inv.name || '').toLowerCase();
            // Exact match or very close match
            return invName === queryLower || 
                   invName.includes(queryLower) || 
                   queryLower.includes(invName) ||
                   // Handle partial matches like "a16z" for "Andreessen Horowitz"
                   invName.split(/\s+/).some(word => word.startsWith(queryLower)) ||
                   // Handle slug-based search
                   (inv.slug || '').toLowerCase().includes(queryLower.replace(/\s+/g, '-'));
        });
        
        if (directMatches.length > 0 && directMatches.length <= 20) {
            // Found direct matches, return them without other filters
            console.log(`Direct name search for "${query}" found ${directMatches.length} matches`);
            
            // Sort by relevance (exact match first, then by name similarity)
            directMatches.sort((a, b) => {
                const aName = (a.name || '').toLowerCase();
                const bName = (b.name || '').toLowerCase();
                // Exact match first
                if (aName === queryLower) return -1;
                if (bName === queryLower) return 1;
                // Then by how close the match is
                const aStartsWith = aName.startsWith(queryLower);
                const bStartsWith = bName.startsWith(queryLower);
                if (aStartsWith && !bStartsWith) return -1;
                if (bStartsWith && !aStartsWith) return 1;
                return aName.localeCompare(bName);
            });
            
            const total = directMatches.length;
            const startIndex = (page - 1) * limit;
            const paginatedResults = directMatches.slice(startIndex, startIndex + limit);
            
            const formattedResults = paginatedResults.map(inv => {
                const team = teamByInvestorId.get(inv.id) || [];
                const teamWithContacts = team.filter(tm => 
                    (tm.emails && tm.emails.length > 0) || 
                    (tm.work_emails && tm.work_emails.length > 0) ||
                    tm.linkedin_url
                );
                const portfolio = investmentsByInvestorId.get(inv.id) || [];
                
                return {
                    id: inv.id,
                    name: inv.name,
                    slug: inv.slug,
                    website: inv.website,
                    description: inv.description?.substring(0, 200),
                    location: [inv.city, inv.region, inv.country].filter(Boolean).join(', '),
                    check_size: {
                        min: inv.check_size_min,
                        max: inv.check_size_max,
                        sweet_spot: inv.sweet_spot
                    },
                    stages: toArray(inv.stages),
                    focus: toArray(inv.focus),
                    tags: toArray(inv.tags).slice(0, 8),
                    verified: inv.verified,
                    contacts_count: teamWithContacts.length,
                    portfolio_count: portfolio.length
                };
            });
            
            return {
                results: formattedResults,
                total,
                page,
                limit,
                total_pages: Math.ceil(total / limit),
                parsed_query: { name: query },
                direct_search: true
            };
        }
    }
    
    // If there's a natural language query, parse it first
    let parsedFilters = {};
    if (query && query.trim()) {
        parsedFilters = await parseNaturalLanguageQuery(query) || {};
    }
    
    // Merge parsed filters with explicit filters (explicit takes precedence)
    const mergedFilters = {
        name: name || parsedFilters.name,
        stages: stages?.length ? stages : parsedFilters.stages,
        focuses: focuses?.length ? focuses : parsedFilters.focuses,
        tags: tags?.length ? tags : parsedFilters.tags,
        locations: locations?.length ? locations : parsedFilters.locations,
        check_size_min: check_size_min ?? parsedFilters.check_size_min,
        check_size_max: check_size_max ?? parsedFilters.check_size_max,
        has_contacts: has_contacts ?? parsedFilters.has_contacts
    };
    
    // If searching by name (from AI parsing), skip location filter for better results
    const isNameSearch = mergedFilters.name && !mergedFilters.stages?.length && !mergedFilters.focuses?.length;
    
    // Filter investors
    let results = investors.filter(inv => {
        // Name search (fuzzy)
        if (mergedFilters.name) {
            const searchName = mergedFilters.name.toLowerCase();
            const invName = (inv.name || '').toLowerCase();
            const invSlug = (inv.slug || '').toLowerCase();
            // More flexible name matching
            const nameMatches = invName.includes(searchName) || 
                               searchName.includes(invName) ||
                               invSlug.includes(searchName.replace(/\s+/g, '-')) ||
                               invName.split(/\s+/).some(word => word.startsWith(searchName));
            if (!nameMatches) {
                return false;
            }
            // If this is primarily a name search, skip other filters
            if (isNameSearch) {
                return true;
            }
        }
        
        // Stage filter
        if (mergedFilters.stages?.length) {
            const invStages = (inv.stages || []).map(s => s.toLowerCase());
            const hasStage = mergedFilters.stages.some(s => 
                invStages.some(is => is.includes(s.toLowerCase()))
            );
            if (!hasStage) return false;
        }
        
        // Focus filter
        if (mergedFilters.focuses?.length) {
            const invFocus = [...toArray(inv.focus), ...toArray(inv.tags)].map(f => f.toLowerCase());
            const hasFocus = mergedFilters.focuses.some(f => 
                invFocus.some(inf => inf.includes(f.toLowerCase()) || f.toLowerCase().includes(inf))
            );
            if (!hasFocus) return false;
        }
        
        // Tags filter
        if (mergedFilters.tags?.length) {
            const invTags = toArray(inv.tags).map(t => t.toLowerCase());
            const hasTag = mergedFilters.tags.some(t => 
                invTags.some(it => it.includes(t.toLowerCase()))
            );
            if (!hasTag) return false;
        }
        
        // Location filter
        if (mergedFilters.locations?.length) {
            const invLocation = [inv.city, inv.region, inv.country].filter(Boolean).join(' ').toLowerCase();
            const hasLocation = mergedFilters.locations.some(loc => 
                invLocation.includes(loc.toLowerCase())
            );
            if (!hasLocation) return false;
        }
        
        // US City filter
        if (us_city) {
            const invCity = (inv.city || '').toLowerCase();
            if (!invCity.includes(us_city.toLowerCase())) return false;
        }
        
        // Check size filter
        if (mergedFilters.check_size_min) {
            const maxCheck = inv.check_size_max || inv.sweet_spot || 0;
            if (maxCheck < mergedFilters.check_size_min) return false;
        }
        if (mergedFilters.check_size_max) {
            const minCheck = inv.check_size_min || 0;
            if (minCheck > mergedFilters.check_size_max) return false;
        }
        
        // Has contacts filter - use index
        if (mergedFilters.has_contacts) {
            const team = teamByInvestorId.get(inv.id) || [];
            const hasContacts = team.some(tm => 
                (tm.emails?.length > 0) || (tm.work_emails?.length > 0)
            );
            if (!hasContacts) return false;
        }
        
        // Verified only filter
        if (verified_only && !inv.verified) {
            return false;
        }
        
        // Recently active filter - check if fund has investments in last N days
        if (recently_active_days) {
            const cutoffDate = new Date();
            cutoffDate.setDate(cutoffDate.getDate() - recently_active_days);
            const portfolio = investmentsByInvestorId.get(inv.id) || [];
            const hasRecentInvestment = portfolio.some(investment => {
                if (!investment.announced_date) return false;
                const investmentDate = new Date(investment.announced_date);
                return investmentDate >= cutoffDate;
            });
            if (!hasRecentInvestment) return false;
        }
        
        return true;
    });
    
    // Sort by relevance (verified first, then by portfolio size) - use indexes
    results.sort((a, b) => {
        if (a.verified !== b.verified) return b.verified ? 1 : -1;
        const aPortfolio = (investmentsByInvestorId.get(a.id) || []).length;
        const bPortfolio = (investmentsByInvestorId.get(b.id) || []).length;
        return bPortfolio - aPortfolio;
    });
    
    // Pagination
    const total = results.length;
    const startIndex = (page - 1) * limit;
    const paginatedResults = results.slice(startIndex, startIndex + limit);
    
    // Format results - use indexes
    const formattedResults = paginatedResults.map(inv => {
        const team = teamByInvestorId.get(inv.id) || [];
        const teamWithContacts = team.filter(tm => 
            (tm.emails?.length > 0) || (tm.work_emails?.length > 0)
        );
        const portfolio = investmentsByInvestorId.get(inv.id) || [];
        
        return {
            id: inv.id,
            name: inv.name,
            slug: inv.slug,
            website: inv.website,
            description: inv.description?.substring(0, 200),
            location: [inv.city, inv.region, inv.country].filter(Boolean).join(', '),
            check_size: {
                min: inv.check_size_min,
                max: inv.check_size_max,
                sweet_spot: inv.sweet_spot
            },
            stages: toArray(inv.stages),
            focus: toArray(inv.focus),
            tags: toArray(inv.tags).slice(0, 8),
            verified: inv.verified,
            contacts_count: teamWithContacts.length,
            portfolio_count: portfolio.length
        };
    });
    
    return {
        results: formattedResults,
        total,
        page,
        limit,
        total_pages: Math.ceil(total / limit),
        parsed_query: query ? parsedFilters : null
    };
}

// Get detailed fund information
function getFundDetails(fundId) {
    const inv = investors.find(i => i.id === fundId || i.slug === fundId);
    if (!inv) return null;
    
    const team = teamMembers.filter(tm => tm.investor_id === inv.id);
    const portfolio = investments.filter(i => i.investor_id === inv.id);
    
    // Get recent investments with founders
    const recentInvestments = portfolio
        .sort((a, b) => new Date(b.announced_date || 0) - new Date(a.announced_date || 0))
        .slice(0, 20)
        .map(investment => {
            const companyFounders = foundersByCompany.get(investment.company_name?.toLowerCase()) || [];
            return {
                company: investment.company_name,
                website: investment.company_website,
                amount: investment.amount_raised,
                date: investment.announced_date,
                city: investment.company_city,
                country: investment.company_country,
                founders: companyFounders.map(f => ({
                    name: f.name,
                    title: f.title,
                    linkedin_url: f.linkedin_url
                }))
            };
        });
    
    return {
        id: inv.id,
        name: inv.name,
        slug: inv.slug,
        website: inv.website,
        description: inv.description,
        thesis: inv.thesis,
        location: [inv.city, inv.region, inv.country].filter(Boolean).join(', '),
        city: inv.city,
        region: inv.region,
        country: inv.country,
        check_size: {
            min: inv.check_size_min,
            max: inv.check_size_max,
            sweet_spot: inv.sweet_spot
        },
        fund_size: inv.fund_size,
        stages: inv.stages || [],
        focus: inv.focus || [],
        tags: inv.tags || [],
        verified: inv.verified,
        profile_url: inv.profile_url,
        linkedin_url: inv.linkedin_url,
        twitter_url: inv.twitter_url,
        team: team.map(tm => ({
            id: tm.id,
            name: tm.name,
            title: tm.title,
            emails: tm.emails || [],
            work_emails: tm.work_emails || [],
            personal_emails: tm.personal_emails || [],
            phones: tm.phones || [],
            linkedin_url: tm.linkedin_url,
            location: tm.location
        })),
        portfolio: recentInvestments,
        portfolio_count: portfolio.length,
        contacts_count: team.filter(tm => 
            (tm.emails?.length > 0) || (tm.work_emails?.length > 0)
        ).length,
        // Founder Approach Guide (enrichment data)
        approach_guide: enrichedInvestors[inv.id] || null
    };
}

// Serve static files
function serveStaticFile(filePath, res) {
    const ext = path.extname(filePath);
    const contentType = MIME_TYPES[ext] || 'application/octet-stream';

    fs.readFile(filePath, (err, content) => {
        if (err) {
            if (err.code === 'ENOENT') {
                res.writeHead(404);
                res.end('File not found');
            } else {
                res.writeHead(500);
                res.end('Server error');
            }
        } else {
            res.writeHead(200, { 'Content-Type': contentType });
            res.end(content);
        }
    });
}

// Send JSON response
function sendJSON(res, statusCode, data, rateLimitInfo = null) {
    const headers = {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*'
    };
    
    // Add rate limit headers if provided
    if (rateLimitInfo) {
        if (rateLimitInfo.limit !== undefined) {
            headers['X-RateLimit-Limit'] = rateLimitInfo.limit;
        }
        if (rateLimitInfo.remaining !== undefined) {
            headers['X-RateLimit-Remaining'] = rateLimitInfo.remaining;
        }
        if (rateLimitInfo.resetIn !== undefined) {
            headers['X-RateLimit-Reset'] = rateLimitInfo.resetIn;
        }
        if (rateLimitInfo.retryAfter !== undefined) {
            headers['Retry-After'] = rateLimitInfo.retryAfter;
        }
    }
    
    res.writeHead(statusCode, headers);
    res.end(JSON.stringify(data));
}

// Send rate limit exceeded response
function sendRateLimitError(res, rateLimitResult) {
    sendJSON(res, 429, { 
        error: rateLimitResult.message,
        retry_after: rateLimitResult.retryAfter 
    }, rateLimitResult);
}

// Main request handler
async function handleRequest(req, res) {
    const url = new URL(req.url, `http://localhost:${PORT}`);
    const clientIP = getClientIP(req);
    
    // Handle CORS preflight (no rate limiting for OPTIONS)
    if (req.method === 'OPTIONS') {
        res.writeHead(204, {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET, POST, PUT, OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type'
        });
        res.end();
        return;
    }
    
    // Skip rate limiting for static files (html, css, js, images)
    const isStaticFile = !url.pathname.startsWith('/api/');
    
    // Apply general rate limiting for API requests
    if (!isStaticFile) {
        const generalLimit = checkRateLimit(clientIP, 'general');
        if (!generalLimit.allowed) {
            sendRateLimitError(res, generalLimit);
            return;
        }
    }
    
    // API: Analyze and search (AI-powered - stricter rate limiting)
    if (url.pathname === '/api/search' && req.method === 'POST') {
        // Apply stricter rate limiting for AI search (expensive operation)
        const searchLimit = checkRateLimit(clientIP, 'search');
        if (!searchLimit.allowed) {
            sendRateLimitError(res, searchLimit);
            return;
        }
        
        let body = '';
        req.on('data', chunk => body += chunk);
        req.on('end', async () => {
            try {
                const { website_url } = JSON.parse(body);
                
                if (!website_url) {
                    sendJSON(res, 400, { error: 'website_url is required' });
                    return;
                }
                
                // Step 1: Scrape website
                const scrapedData = await scrapeWebsite(website_url);
                
                // Step 2: Analyze with AI
                const companyAnalysis = await analyzeCompany(scrapedData);
                
                // Step 3: Match investors
                const matches = matchInvestors(companyAnalysis);
                
                // Step 4: Format and return results
                const results = formatResults(matches, companyAnalysis);
                
                // Track search in user history and cache results if logged in
                const sessionToken = getSessionFromCookie(req);
                const sessionUser = sessionToken ? findUserBySession(sessionToken) : null;
                if (sessionUser) {
                    addSearchToHistory(sessionUser.id, {
                        query: website_url,
                        company_name: companyAnalysis.name || website_url,
                        matches_count: matches.length
                    });
                    
                    // Cache the results for the user's company website
                    const user = findUserById(sessionUser.id);
                    if (user && user.company_website && normalizeUrl(website_url) === normalizeUrl(user.company_website)) {
                        user.cached_results = {
                            results: results,
                            cached_at: new Date().toISOString()
                        };
                        saveUsers();
                    }
                }
                
                sendJSON(res, 200, results);
                
            } catch (error) {
                console.error('Search error:', error);
                sendJSON(res, 500, { error: error.message });
            }
        });
        return;
    }
    
    // API: Get database stats
    if (url.pathname === '/api/stats' && req.method === 'GET') {
        sendJSON(res, 200, {
            investors: investors.length,
            team_members: teamMembers.length,
            team_with_emails: teamMembers.filter(t => t.emails?.length > 0 || t.work_emails?.length > 0).length,
            investments: investments.length
        });
        return;
    }
    
    // API: Get cached investor results for user's company
    if (url.pathname === '/api/my-investors' && req.method === 'GET') {
        const sessionToken = getSessionFromCookie(req);
        const sessionUser = sessionToken ? findUserBySession(sessionToken) : null;
        
        if (!sessionUser) {
            sendJSON(res, 401, { error: 'Not authenticated' });
            return;
        }
        
        const user = findUserById(sessionUser.id);
        if (!user) {
            sendJSON(res, 404, { error: 'User not found' });
            return;
        }
        
        if (user.cached_results) {
            sendJSON(res, 200, {
                has_cached: true,
                company_website: user.company_website,
                cached_at: user.cached_results.cached_at,
                ...user.cached_results.results
            });
        } else {
            sendJSON(res, 200, {
                has_cached: false,
                company_website: user.company_website
            });
        }
        return;
    }
    
    // API: Create Stripe Checkout Session for registration
    if (url.pathname === '/api/create-checkout-session' && req.method === 'POST') {
        // Apply auth rate limiting
        const authLimit = checkRateLimit(clientIP, 'auth');
        if (!authLimit.allowed) {
            sendRateLimitError(res, authLimit);
            return;
        }
        
        let body = '';
        req.on('data', chunk => body += chunk);
        req.on('end', async () => {
            try {
                const { email, password, company_website } = JSON.parse(body);
                
                // Validation
                if (!email || !password || !company_website) {
                    sendJSON(res, 400, { error: 'Email, password, and company website are required' });
                    return;
                }
                
                if (!email.includes('@')) {
                    sendJSON(res, 400, { error: 'Invalid email format' });
                    return;
                }
                
                if (password.length < 6) {
                    sendJSON(res, 400, { error: 'Password must be at least 6 characters' });
                    return;
                }
                
                // Check if user exists
                if (findUserByEmail(email)) {
                    sendJSON(res, 400, { error: 'An account with this email already exists' });
                    return;
                }
                
                // Store pending registration data (will be completed after payment)
                const pendingId = crypto.randomUUID();
                const pendingUser = {
                    id: pendingId,
                    email: email.toLowerCase(),
                    password_hash: hashPassword(password),
                    company_website: company_website,
                    status: 'pending',
                    created_at: new Date().toISOString()
                };
                
                // Create Stripe Checkout Session
                const session = await stripe.checkout.sessions.create({
                    payment_method_types: ['card'],
                    line_items: [{
                        price_data: {
                            currency: 'usd',
                            product_data: {
                                name: 'Investor Match - 1 Year Access',
                                description: 'Full access to 19K+ investor database with contact info'
                            },
                            unit_amount: STRIPE_PRICE_AMOUNT,
                        },
                        quantity: 1,
                    }],
                    mode: 'payment',
                    success_url: `http://localhost:${PORT}/payment-success.html?session_id={CHECKOUT_SESSION_ID}`,
                    cancel_url: `http://localhost:${PORT}/register.html?canceled=true`,
                    customer_email: email,
                    metadata: {
                        pending_user_id: pendingId,
                        email: email.toLowerCase(),
                        password_hash: pendingUser.password_hash,
                        company_website: company_website
                    }
                });
                
                sendJSON(res, 200, { 
                    sessionId: session.id,
                    url: session.url 
                });
                
            } catch (error) {
                console.error('Stripe checkout error:', error);
                sendJSON(res, 500, { error: 'Failed to create checkout session' });
            }
        });
        return;
    }
    
    // API: Verify payment and complete registration
    if (url.pathname === '/api/verify-payment' && req.method === 'POST') {
        let body = '';
        req.on('data', chunk => body += chunk);
        req.on('end', async () => {
            try {
                const { session_id } = JSON.parse(body);
                
                if (!session_id) {
                    sendJSON(res, 400, { error: 'Session ID is required' });
                    return;
                }
                
                // Retrieve the checkout session from Stripe
                const session = await stripe.checkout.sessions.retrieve(session_id);
                
                if (session.payment_status !== 'paid') {
                    sendJSON(res, 400, { error: 'Payment not completed' });
                    return;
                }
                
                // Check if user already exists (prevent duplicate registrations)
                const existingUser = findUserByEmail(session.metadata.email);
                if (existingUser) {
                    // User already registered, just log them in
                    const sessionToken = generateSessionToken();
                    sessions.set(sessionToken, { 
                        id: existingUser.id, 
                        email: existingUser.email, 
                        company_website: existingUser.company_website 
                    });
                    setSessionCookie(res, sessionToken);
                    sendJSON(res, 200, { 
                        success: true, 
                        user: { email: existingUser.email, company_website: existingUser.company_website }
                    });
                    return;
                }
                
                // Create the user account
                const user = {
                    id: session.metadata.pending_user_id,
                    email: session.metadata.email,
                    password_hash: session.metadata.password_hash,
                    company_website: session.metadata.company_website,
                    status: 'active',
                    payment_id: session.payment_intent,
                    paid_at: new Date().toISOString(),
                    expires_at: new Date(Date.now() + 365 * 24 * 60 * 60 * 1000).toISOString(), // 1 year
                    created_at: new Date().toISOString()
                };
                
                users.push(user);
                saveUsers();
                
                // Create session and log in
                const sessionToken = generateSessionToken();
                sessions.set(sessionToken, { id: user.id, email: user.email, company_website: user.company_website });
                setSessionCookie(res, sessionToken);
                
                console.log(`✅ New paid user registered: ${user.email}`);
                
                sendJSON(res, 200, { 
                    success: true, 
                    user: { email: user.email, company_website: user.company_website }
                });
                
            } catch (error) {
                console.error('Payment verification error:', error);
                sendJSON(res, 500, { error: 'Failed to verify payment' });
            }
        });
        return;
    }
    
    // API: Get Stripe publishable key
    if (url.pathname === '/api/stripe-key' && req.method === 'GET') {
        sendJSON(res, 200, { publishableKey: STRIPE_PUBLISHABLE_KEY });
        return;
    }
    
    // API: Get user profile
    if (url.pathname === '/api/profile' && req.method === 'GET') {
        const sessionToken = getSessionFromCookie(req);
        const sessionUser = sessionToken ? findUserBySession(sessionToken) : null;
        
        if (!sessionUser) {
            sendJSON(res, 401, { error: 'Please login to view profile' });
            return;
        }
        
        const user = findUserById(sessionUser.id);
        if (!user) {
            sendJSON(res, 404, { error: 'User not found' });
            return;
        }
        
        // Calculate subscription status
        const now = new Date();
        const expiresAt = user.expires_at ? new Date(user.expires_at) : null;
        const isActive = user.status === 'active' && (!expiresAt || expiresAt > now);
        const daysRemaining = expiresAt ? Math.max(0, Math.ceil((expiresAt - now) / (1000 * 60 * 60 * 24))) : null;
        
        sendJSON(res, 200, {
            id: user.id,
            email: user.email,
            company_website: user.company_website,
            created_at: user.created_at,
            subscription: {
                status: user.status || 'free',
                is_active: isActive,
                paid_at: user.paid_at || null,
                expires_at: user.expires_at || null,
                days_remaining: daysRemaining,
                canceled_at: user.canceled_at || null
            },
            search_history: (user.search_history || []).slice(0, 10)
        });
        return;
    }
    
    // API: Update user profile
    if (url.pathname === '/api/profile' && req.method === 'PUT') {
        const sessionToken = getSessionFromCookie(req);
        const sessionUser = sessionToken ? findUserBySession(sessionToken) : null;
        
        if (!sessionUser) {
            sendJSON(res, 401, { error: 'Please login to update profile' });
            return;
        }
        
        let body = '';
        req.on('data', chunk => body += chunk);
        req.on('end', () => {
            try {
                const { company_website } = JSON.parse(body);
                
                const user = findUserById(sessionUser.id);
                if (!user) {
                    sendJSON(res, 404, { error: 'User not found' });
                    return;
                }
                
                if (company_website) {
                    user.company_website = company_website;
                }
                
                saveUsers();
                
                // Update session
                sessions.set(sessionToken, { 
                    id: user.id, 
                    email: user.email, 
                    company_website: user.company_website 
                });
                
                sendJSON(res, 200, { success: true, company_website: user.company_website });
            } catch (error) {
                sendJSON(res, 500, { error: 'Failed to update profile' });
            }
        });
        return;
    }
    
    // API: Cancel subscription
    if (url.pathname === '/api/cancel-subscription' && req.method === 'POST') {
        const sessionToken = getSessionFromCookie(req);
        const sessionUser = sessionToken ? findUserBySession(sessionToken) : null;
        
        if (!sessionUser) {
            sendJSON(res, 401, { error: 'Please login to cancel subscription' });
            return;
        }
        
        const user = findUserById(sessionUser.id);
        if (!user) {
            sendJSON(res, 404, { error: 'User not found' });
            return;
        }
        
        if (user.status !== 'active') {
            sendJSON(res, 400, { error: 'No active subscription to cancel' });
            return;
        }
        
        // Mark as canceled but keep access until expiry
        user.status = 'canceled';
        user.canceled_at = new Date().toISOString();
        saveUsers();
        
        console.log(`⚠️ Subscription canceled: ${user.email}`);
        
        sendJSON(res, 200, { 
            success: true, 
            message: 'Subscription canceled. You will retain access until your current period ends.',
            expires_at: user.expires_at
        });
        return;
    }
    
    // API: Delete search from history
    if (url.pathname === '/api/delete-search' && req.method === 'POST') {
        const sessionToken = getSessionFromCookie(req);
        const sessionUser = sessionToken ? findUserBySession(sessionToken) : null;
        
        if (!sessionUser) {
            sendJSON(res, 401, { error: 'Please login' });
            return;
        }
        
        let body = '';
        req.on('data', chunk => body += chunk);
        req.on('end', () => {
            try {
                const { search_id } = JSON.parse(body);
                
                const user = findUserById(sessionUser.id);
                if (!user || !user.search_history) {
                    sendJSON(res, 404, { error: 'Not found' });
                    return;
                }
                
                user.search_history = user.search_history.filter(s => s.id !== search_id);
                saveUsers();
                
                sendJSON(res, 200, { success: true });
            } catch (error) {
                sendJSON(res, 500, { error: 'Failed to delete search' });
            }
        });
        return;
    }
    
    // API: Register new user (FREE - keep for testing)
    if (url.pathname === '/api/register' && req.method === 'POST') {
        // Apply auth rate limiting
        const authLimit = checkRateLimit(clientIP, 'auth');
        if (!authLimit.allowed) {
            sendRateLimitError(res, authLimit);
            return;
        }
        
        let body = '';
        req.on('data', chunk => body += chunk);
        req.on('end', () => {
            try {
                const { email, password, company_website } = JSON.parse(body);
                
                // Validation
                if (!email || !password || !company_website) {
                    sendJSON(res, 400, { error: 'Email, password, and company website are required' });
                    return;
                }
                
                if (!email.includes('@')) {
                    sendJSON(res, 400, { error: 'Invalid email format' });
                    return;
                }
                
                if (password.length < 6) {
                    sendJSON(res, 400, { error: 'Password must be at least 6 characters' });
                    return;
                }
                
                // Check if user exists
                if (findUserByEmail(email)) {
                    sendJSON(res, 400, { error: 'An account with this email already exists' });
                    return;
                }
                
                // Create user
                const user = {
                    id: crypto.randomUUID(),
                    email: email.toLowerCase(),
                    password_hash: hashPassword(password),
                    company_website: company_website,
                    created_at: new Date().toISOString()
                };
                
                users.push(user);
                saveUsers();
                
                // Create session
                const sessionToken = generateSessionToken();
                sessions.set(sessionToken, { id: user.id, email: user.email, company_website: user.company_website });
                setSessionCookie(res, sessionToken);
                
                sendJSON(res, 201, { 
                    success: true, 
                    user: { email: user.email, company_website: user.company_website }
                });
                
            } catch (error) {
                console.error('Registration error:', error);
                sendJSON(res, 500, { error: 'Registration failed' });
            }
        });
        return;
    }
    
    // API: Login
    if (url.pathname === '/api/login' && req.method === 'POST') {
        // Apply auth rate limiting (prevents brute force attacks)
        const authLimit = checkRateLimit(clientIP, 'auth');
        if (!authLimit.allowed) {
            sendRateLimitError(res, authLimit);
            return;
        }
        
        let body = '';
        req.on('data', chunk => body += chunk);
        req.on('end', () => {
            try {
                const { email, password } = JSON.parse(body);
                
                if (!email || !password) {
                    sendJSON(res, 400, { error: 'Email and password are required' });
                    return;
                }
                
                const user = findUserByEmail(email);
                if (!user || user.password_hash !== hashPassword(password)) {
                    sendJSON(res, 401, { error: 'Invalid email or password' });
                    return;
                }
                
                // Create session
                const sessionToken = generateSessionToken();
                sessions.set(sessionToken, { id: user.id, email: user.email, company_website: user.company_website });
                setSessionCookie(res, sessionToken);
                
                sendJSON(res, 200, { 
                    success: true, 
                    user: { email: user.email, company_website: user.company_website }
                });
                
            } catch (error) {
                console.error('Login error:', error);
                sendJSON(res, 500, { error: 'Login failed' });
            }
        });
        return;
    }
    
    // API: Logout
    if (url.pathname === '/api/logout' && req.method === 'POST') {
        const sessionToken = getSessionFromCookie(req);
        if (sessionToken) {
            sessions.delete(sessionToken);
        }
        clearSessionCookie(res);
        sendJSON(res, 200, { success: true });
        return;
    }
    
    // API: Get current user
    if (url.pathname === '/api/me' && req.method === 'GET') {
        const sessionToken = getSessionFromCookie(req);
        const user = sessionToken ? findUserBySession(sessionToken) : null;
        
        if (user) {
            sendJSON(res, 200, { 
                authenticated: true, 
                user: { email: user.email, company_website: user.company_website }
            });
        } else {
            sendJSON(res, 200, { authenticated: false });
        }
        return;
    }
    
    // API: Get filter options (must be before /api/funds/:id)
    if (url.pathname === '/api/funds/filters' && req.method === 'GET') {
        sendJSON(res, 200, getFilterOptions());
        return;
    }
    
    // API: Search funds (available for all users, but non-auth users get limited results)
    if (url.pathname === '/api/funds/search' && req.method === 'POST') {
        const sessionToken = getSessionFromCookie(req);
        const user = sessionToken ? findUserBySession(sessionToken) : null;
        
        let body = '';
        req.on('data', chunk => body += chunk);
        req.on('end', async () => {
            try {
                const filters = JSON.parse(body);
                const results = await searchFunds(filters);
                
                // Track fund search in user history if there's a query (only for authenticated users)
                if (user && filters.query && filters.query.trim()) {
                    addSearchToHistory(user.id, {
                        query: filters.query,
                        company_name: `🔍 Fund Search: "${filters.query}"`,
                        matches_count: results.total,
                        type: 'fund_search'
                    });
                }
                
                sendJSON(res, 200, results);
            } catch (error) {
                console.error('Fund search error:', error);
                sendJSON(res, 500, { error: error.message });
            }
        });
        return;
    }
    
    // API: Get fund details (available to all users for preview) - must be last of /api/funds/* routes
    if (url.pathname.startsWith('/api/funds/') && req.method === 'GET') {
        const fundId = url.pathname.replace('/api/funds/', '');
        const fund = getFundDetails(fundId);
        
        if (fund) {
            sendJSON(res, 200, fund);
        } else {
            sendJSON(res, 404, { error: 'Fund not found' });
        }
        return;
    }
    
    // Serve static files
    let filePath = path.join(__dirname, url.pathname === '/' ? 'index.html' : url.pathname);
    serveStaticFile(filePath, res);
}

// Create and start server
const server = http.createServer(handleRequest);

// Start the application
initialize().then(() => {
    server.listen(PORT, () => {
        console.log(`\n🚀 Investor Match running at http://localhost:${PORT}\n`);
        console.log('Endpoints:');
        console.log('  GET  /           → Main application');
        console.log('  POST /api/search → Analyze website and find investors');
        console.log('  GET  /api/stats  → Database statistics\n');
    });
}).catch(err => {
    console.error('Failed to initialize:', err);
    process.exit(1);
});

