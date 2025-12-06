#!/usr/bin/env node
/**
 * Investor Enrichment Script
 * 
 * Researches US investors using Tavily web search and AI to generate
 * "Founder Approach Guides" with insights on how to pitch each investor.
 * 
 * Usage:
 *   node investor-enrichment.js [options]
 * 
 * Options:
 *   --limit <n>      Maximum investors to enrich (default: 50)
 *   --resume         Resume from last progress
 *   --dry-run        Show what would be enriched without making API calls
 */

const fs = require('fs');
const path = require('path');
const https = require('https');
require('dotenv').config();

// Configuration
const TAVILY_API_KEY = process.env.TAVILY_API_KEY;
const OPENAI_API_KEY = process.env.OPENAI_API_KEY;

// Paths
const DATA_DIR = path.join(__dirname, '..', 'results', 'unified_database');
const INVESTORS_PATH = path.join(DATA_DIR, 'investors.json');
const TEAM_MEMBERS_PATH = path.join(DATA_DIR, 'team_members.json');
const PROGRESS_PATH = path.join(DATA_DIR, '.enrichment_progress.json');
const ENRICHED_PATH = path.join(DATA_DIR, 'enriched_investors.json');
const RESEARCH_CACHE_PATH = path.join(DATA_DIR, '.research_cache.json');

// Parse command line arguments
const args = process.argv.slice(2);
const getArg = (name, defaultValue) => {
    const idx = args.indexOf(name);
    if (idx === -1) return defaultValue;
    if (typeof defaultValue === 'boolean') return true;
    return args[idx + 1] || defaultValue;
};

const LIMIT = parseInt(getArg('--limit', '50'), 10);
const RESUME = getArg('--resume', false);
const DRY_RUN = getArg('--dry-run', false);
const DELAY_MS = 2000; // Delay between enrichments to avoid rate limits

// ============================================================================
// HTTP Request Helpers
// ============================================================================

function makeRequest(url, options, postData = null) {
    return new Promise((resolve, reject) => {
        const urlObj = new URL(url);
        const reqOptions = {
            hostname: urlObj.hostname,
            port: urlObj.port || 443,
            path: urlObj.pathname + urlObj.search,
            method: options.method || 'GET',
            headers: options.headers || {}
        };

        const req = https.request(reqOptions, (res) => {
            let data = '';
            res.on('data', chunk => data += chunk);
            res.on('end', () => {
                try {
                    resolve({ status: res.statusCode, data: JSON.parse(data) });
                } catch (e) {
                    resolve({ status: res.statusCode, data: data });
                }
            });
        });

        req.on('error', reject);
        
        if (postData) {
            req.write(postData);
        }
        req.end();
    });
}

// ============================================================================
// Tavily Web Search
// ============================================================================

async function searchTavily(query) {
    try {
        const response = await makeRequest(
            'https://api.tavily.com/search',
            {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                }
            },
            JSON.stringify({
                api_key: TAVILY_API_KEY,
                query: query,
                search_depth: 'basic',
                include_answer: false,
                include_raw_content: false,
                max_results: 5
            })
        );

        if (response.status === 200 && response.data.results) {
            return response.data.results.map(r => ({
                title: r.title,
                url: r.url,
                content: r.content
            }));
        }
        
        console.error(`   ⚠️  Tavily search failed: ${response.status}`);
        return [];
    } catch (error) {
        console.error(`   ⚠️  Tavily error: ${error.message}`);
        return [];
    }
}

async function researchInvestor(investor, teamMembers) {
    const queries = [];
    
    // Search for the fund/firm
    queries.push(`"${investor.name}" venture capital investment thesis`);
    queries.push(`"${investor.name}" VC what they look for startups`);
    
    // Search for key team members (partners, managing directors)
    const keyMembers = teamMembers
        .filter(tm => tm.investor_id === investor.id)
        .filter(tm => {
            const title = (tm.title || '').toLowerCase();
            return title.includes('partner') || 
                   title.includes('managing') || 
                   title.includes('principal') ||
                   title.includes('founder') ||
                   title.includes('general');
        })
        .slice(0, 2); // Top 2 key members
    
    for (const member of keyMembers) {
        queries.push(`"${member.name}" investor interview podcast`);
    }
    
    // Execute all searches
    const allResults = [];
    for (const query of queries) {
        console.log(`   🔍 Searching: ${query.substring(0, 60)}...`);
        const results = await searchTavily(query);
        allResults.push(...results);
        await sleep(500); // Small delay between searches
    }
    
    // Deduplicate by URL
    const seen = new Set();
    const uniqueResults = allResults.filter(r => {
        if (seen.has(r.url)) return false;
        seen.add(r.url);
        return true;
    });
    
    return {
        sources: uniqueResults.map(r => r.url),
        content: uniqueResults.map(r => `${r.title}\n${r.content}`).join('\n\n')
    };
}

// ============================================================================
// OpenAI GPT-4o Analysis
// ============================================================================

async function analyzeWithGPT(investor, teamMembers, researchContent, retryCount = 0) {
    const MAX_RETRIES = 5;
    const BASE_DELAY = 2000; // 2 seconds
    
    const keyMembers = teamMembers
        .filter(tm => tm.investor_id === investor.id)
        .slice(0, 5)
        .map(tm => `- ${tm.name}: ${tm.title}`)
        .join('\n');
    
    const prompt = `You are an expert startup advisor helping founders approach venture capital investors. Based on the research provided, create a comprehensive "Founder Approach Guide" for pitching to this investor.

INVESTOR INFORMATION:
- Fund Name: ${investor.name}
- Location: ${[investor.city, investor.region, investor.country].filter(Boolean).join(', ')}
- Investment Stages: ${Array.isArray(investor.stages) ? investor.stages.join(', ') : (investor.stages || 'Not specified')}
- Focus Areas: ${Array.isArray(investor.focus) ? investor.focus.join(', ') : (investor.focus || 'Not specified')}
- Tags: ${Array.isArray(investor.tags) ? investor.tags.join(', ') : (investor.tags || 'Not specified')}
- Description: ${investor.description || 'Not available'}
- Check Size: ${investor.check_size_min && investor.check_size_max ? `$${investor.check_size_min/1000}K - $${investor.check_size_max/1000}K` : 'Not specified'}

KEY TEAM MEMBERS:
${keyMembers || 'Not available'}

RESEARCH FINDINGS:
${researchContent || 'Limited research available'}

Based on this information, provide a JSON response with the following structure:
{
  "summary": "A 2-3 sentence overview of this investor and what makes them unique",
  "investment_thesis": "What is their investment philosophy and thesis? What types of companies do they typically invest in?",
  "what_excites_them": "What gets this investor excited? What patterns do they look for in founders and companies?",
  "approach_tips": ["tip1", "tip2", "tip3", "tip4", "tip5"],
  "red_flags": ["red_flag1", "red_flag2", "red_flag3"],
  "talking_points": ["point1", "point2", "point3", "point4"],
  "personal_interests": ["interest1", "interest2"] 
}

Guidelines:
- Be specific and actionable, not generic
- If research is limited, make reasonable inferences from their focus areas and portfolio
- approach_tips should be concrete actions founders can take
- red_flags should warn about things to avoid
- talking_points should be conversation starters based on their interests/portfolio
- personal_interests can include hobbies, causes they care about, or non-work interests (only if found in research)

Respond ONLY with valid JSON, no additional text.`;

    try {
        const response = await makeRequest(
            'https://api.openai.com/v1/chat/completions',
            {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'Authorization': `Bearer ${OPENAI_API_KEY}`
                }
            },
            JSON.stringify({
                model: 'gpt-4o',
                messages: [
                    { role: 'system', content: 'You are a helpful assistant that outputs only valid JSON.' },
                    { role: 'user', content: prompt }
                ],
                temperature: 0.7,
                max_tokens: 1500
            })
        );

        if (response.status === 200 && response.data.choices?.[0]?.message?.content) {
            const content = response.data.choices[0].message.content;
            // Extract JSON from response (handle markdown code blocks)
            const jsonMatch = content.match(/\{[\s\S]*\}/);
            if (jsonMatch) {
                return JSON.parse(jsonMatch[0]);
            }
        }
        
        // Handle rate limiting (429) with exponential backoff
        if (response.status === 429 && retryCount < MAX_RETRIES) {
            const delay = BASE_DELAY * Math.pow(2, retryCount); // Exponential backoff: 2s, 4s, 8s, 16s, 32s
            console.log(`   ⏳ Rate limited (429). Waiting ${delay/1000}s before retry ${retryCount + 1}/${MAX_RETRIES}...`);
            await sleep(delay);
            return analyzeWithGPT(investor, teamMembers, researchContent, retryCount + 1);
        }
        
        console.error(`   ⚠️  OpenAI analysis failed: ${response.status}`);
        return null;
    } catch (error) {
        console.error(`   ⚠️  OpenAI error: ${error.message}`);
        return null;
    }
}

// ============================================================================
// Progress Management
// ============================================================================

function loadProgress() {
    try {
        if (fs.existsSync(PROGRESS_PATH)) {
            return JSON.parse(fs.readFileSync(PROGRESS_PATH, 'utf-8'));
        }
    } catch (e) {}
    return { enriched_ids: [], last_updated: null };
}

function saveProgress(progress) {
    progress.last_updated = new Date().toISOString();
    fs.writeFileSync(PROGRESS_PATH, JSON.stringify(progress, null, 2));
}

function loadEnrichedData() {
    try {
        if (fs.existsSync(ENRICHED_PATH)) {
            return JSON.parse(fs.readFileSync(ENRICHED_PATH, 'utf-8'));
        }
    } catch (e) {}
    return {};
}

function saveEnrichedData(data) {
    fs.writeFileSync(ENRICHED_PATH, JSON.stringify(data, null, 2));
}

function loadResearchCache() {
    try {
        if (fs.existsSync(RESEARCH_CACHE_PATH)) {
            return JSON.parse(fs.readFileSync(RESEARCH_CACHE_PATH, 'utf-8'));
        }
    } catch (e) {}
    return {};
}

function saveResearchCache(cache) {
    fs.writeFileSync(RESEARCH_CACHE_PATH, JSON.stringify(cache, null, 2));
}

// ============================================================================
// Utility Functions
// ============================================================================

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

function formatDuration(ms) {
    const seconds = Math.floor(ms / 1000);
    const minutes = Math.floor(seconds / 60);
    const hours = Math.floor(minutes / 60);
    
    if (hours > 0) return `${hours}h ${minutes % 60}m`;
    if (minutes > 0) return `${minutes}m ${seconds % 60}s`;
    return `${seconds}s`;
}

// ============================================================================
// Main Enrichment Pipeline
// ============================================================================

async function main() {
    console.log('\n╔══════════════════════════════════════════════════════════════════════════════╗');
    console.log('║               Investor Enrichment Pipeline (Tavily + AI)                      ║');
    console.log('╚══════════════════════════════════════════════════════════════════════════════╝\n');

    // Load data
    console.log('📂 Loading data...');
    const investors = JSON.parse(fs.readFileSync(INVESTORS_PATH, 'utf-8'));
    const teamMembers = JSON.parse(fs.readFileSync(TEAM_MEMBERS_PATH, 'utf-8'));
    console.log(`   ✅ Loaded ${investors.length} investors`);
    console.log(`   ✅ Loaded ${teamMembers.length} team members`);

    // Filter to US investors
    const usInvestors = investors.filter(inv => inv.country === 'United States');
    console.log(`   🇺🇸 ${usInvestors.length} US investors found`);

    // Load progress
    const progress = RESUME ? loadProgress() : { enriched_ids: [], last_updated: null };
    const enrichedData = RESUME ? loadEnrichedData() : {};
    
    if (RESUME && progress.enriched_ids.length > 0) {
        console.log(`   🔄 Resuming from previous run (${progress.enriched_ids.length} already enriched)`);
    }

    // Prioritize investors with team members who have emails (more likely to be contacted)
    const investorPriority = usInvestors.map(inv => {
        const team = teamMembers.filter(tm => tm.investor_id === inv.id);
        const hasEmails = team.some(tm => tm.emails?.length > 0);
        const keyMemberCount = team.filter(tm => {
            const title = (tm.title || '').toLowerCase();
            return title.includes('partner') || title.includes('managing') || title.includes('principal');
        }).length;
        return {
            investor: inv,
            score: (hasEmails ? 10 : 0) + keyMemberCount + (inv.verified ? 5 : 0)
        };
    }).sort((a, b) => b.score - a.score);

    // Filter out already enriched and apply limit
    const toEnrich = investorPriority
        .filter(p => !progress.enriched_ids.includes(p.investor.id))
        .slice(0, LIMIT)
        .map(p => p.investor);

    console.log(`\n📊 Enrichment Plan:`);
    console.log(`   • Target: ${LIMIT} investors`);
    console.log(`   • Already enriched: ${progress.enriched_ids.length}`);
    console.log(`   • To enrich this run: ${toEnrich.length}`);
    console.log(`   • Estimated time: ${formatDuration(toEnrich.length * (DELAY_MS + 5000))}`);

    if (DRY_RUN) {
        console.log('\n🔍 DRY RUN - Would enrich these investors:');
        toEnrich.slice(0, 10).forEach((inv, i) => {
            console.log(`   ${i + 1}. ${inv.name} (${inv.city || 'Unknown location'})`);
        });
        if (toEnrich.length > 10) {
            console.log(`   ... and ${toEnrich.length - 10} more`);
        }
        return;
    }

    if (toEnrich.length === 0) {
        console.log('\n✅ No investors to enrich. All done!');
        return;
    }

    // Load research cache (Tavily results)
    const researchCache = loadResearchCache();
    const cachedCount = Object.keys(researchCache).length;
    if (cachedCount > 0) {
        console.log(`   📦 Research cache: ${cachedCount} investors cached (will skip Tavily for these)`);
    }

    // Start enrichment
    console.log('\n🚀 Starting enrichment...\n');
    const startTime = Date.now();
    let successCount = 0;
    let failCount = 0;
    let cacheHits = 0;

    for (let i = 0; i < toEnrich.length; i++) {
        const investor = toEnrich[i];
        const progressPct = Math.round(((i + 1) / toEnrich.length) * 100);
        
        console.log(`\n[${i + 1}/${toEnrich.length}] (${progressPct}%) ${investor.name}`);
        console.log(`   📍 ${[investor.city, investor.region].filter(Boolean).join(', ')}`);

        try {
            let research;
            
            // Step 1: Check cache or do web research
            if (researchCache[investor.id]) {
                // Use cached Tavily results
                research = researchCache[investor.id];
                console.log(`   📦 Using cached research (${research.sources.length} sources)`);
                cacheHits++;
            } else {
                // Do fresh Tavily research
                console.log('   📚 Researching...');
                research = await researchInvestor(investor, teamMembers);
                console.log(`   ✅ Found ${research.sources.length} sources`);
                
                // Save to cache immediately (even if GPT fails later)
                researchCache[investor.id] = research;
                saveResearchCache(researchCache);
            }

            // Step 2: AI analysis
            console.log('   🤖 Analyzing with AI...');
            const analysis = await analyzeWithGPT(investor, teamMembers, research.content);
            
            // Add delay after GPT call to prevent rate limiting
            await sleep(1500);

            if (analysis) {
                // Save enrichment data
                enrichedData[investor.id] = {
                    researched_at: new Date().toISOString(),
                    sources: research.sources,
                    raw_research_content: research.content,
                    ...analysis
                };
                
                progress.enriched_ids.push(investor.id);
                saveProgress(progress);
                saveEnrichedData(enrichedData);
                
                // Remove from cache after successful enrichment (no longer needed)
                delete researchCache[investor.id];
                saveResearchCache(researchCache);
                
                successCount++;
                console.log(`   ✅ Enriched successfully`);
                console.log(`   💡 Summary: ${analysis.summary?.substring(0, 100)}...`);
            } else {
                failCount++;
                console.log(`   ❌ Analysis failed (research cached for retry)`);
            }

        } catch (error) {
            failCount++;
            console.error(`   ❌ Error: ${error.message}`);
        }

        // Delay before next investor
        if (i < toEnrich.length - 1) {
            await sleep(DELAY_MS);
        }
    }
    
    if (cacheHits > 0) {
        console.log(`\n📦 Cache stats: ${cacheHits} Tavily calls saved by using cached research`);
    }

    // Summary
    const elapsed = Date.now() - startTime;
    console.log('\n╔══════════════════════════════════════════════════════════════════════════════╗');
    console.log('║                           Enrichment Complete                                ║');
    console.log('╚══════════════════════════════════════════════════════════════════════════════╝');
    console.log(`\n📊 Results:`);
    console.log(`   ✅ Successfully enriched: ${successCount}`);
    console.log(`   ❌ Failed: ${failCount}`);
    console.log(`   ⏱️  Total time: ${formatDuration(elapsed)}`);
    console.log(`   📁 Data saved to: ${ENRICHED_PATH}`);
    console.log(`\n💡 To continue enriching more investors, run with --resume flag\n`);
}

// Run
main().catch(console.error);

