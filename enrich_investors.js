#!/usr/bin/env node
/**
 * Investor Team Member Enrichment Script
 * 
 * Uses ContactOut API to enrich team member data with:
 * - Personal emails
 * - Work emails
 * - Phone numbers
 * - Full profile details (experience, education, skills, etc.)
 * 
 * Usage:
 *   node enrich_investors.js [options]
 * 
 * Options:
 *   --limit <n>      Maximum profiles to enrich (default: all)
 *   --delay <ms>     Delay between API calls (default: 1000)
 *   --resume         Resume from last progress
 *   --dry-run        Show what would be enriched without making API calls
 */

const https = require('https');
const fs = require('fs');
const path = require('path');

// ============================================================================
// Configuration
// ============================================================================

const CONFIG = {
    // ContactOut API settings
    API_TOKEN: 'WU3IKA3NVipp7ftOxEmk3CB2',
    API_BASE: 'api.contactout.com',
    
    // Rate limiting (ContactOut allows 1000 req/min for this endpoint)
    DEFAULT_DELAY: 1000,        // 1 second between requests (safe)
    PAUSE_EVERY: 50,            // Pause every 50 enrichments
    PAUSE_DURATION: 10000,      // 10 second pause
    
    // Retry settings - INFINITE for overnight runs
    MAX_RETRIES: Infinity,      // Never give up on a single request
    INITIAL_RETRY_DELAY: 5000,  // 5 seconds
    MAX_RETRY_DELAY: 300000,    // Max 5 minutes between retries
    
    // File paths
    INPUT_DIR: path.join(__dirname, 'results', 'visible_db_export'),
    OUTPUT_DIR: path.join(__dirname, 'results', 'enriched_investors'),
    PROGRESS_FILE: path.join(__dirname, 'results', 'enriched_investors', '.enrichment_progress.json'),
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
        resume: false,
        dryRun: false,
    };

    for (let i = 0; i < args.length; i++) {
        switch (args[i]) {
            case '--limit': options.limit = parseInt(args[++i], 10); break;
            case '--delay': options.delay = parseInt(args[++i], 10); break;
            case '--resume': options.resume = true; break;
            case '--dry-run': options.dryRun = true; break;
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
║              Investor Team Member Enrichment (ContactOut API)                ║
║                     🌙 Designed for overnight runs 🌙                        ║
╚══════════════════════════════════════════════════════════════════════════════╝

Enriches team member LinkedIn profiles with emails, phones, and full details.
Only processes US-based fund team members.

USAGE:
  node enrich_investors.js [options]

OPTIONS:
  --limit <n>      Maximum profiles to enrich (default: all with LinkedIn)
  --delay <ms>     Delay between API calls in ms (default: ${CONFIG.DEFAULT_DELAY})
  --resume         Resume from last saved progress
  --dry-run        Preview what would be enriched without API calls
  --help, -h       Show this help

BULLETPROOF FEATURES:
  ✓ Never crashes - infinite retries with exponential backoff
  ✓ Auto-saves progress every 10 profiles
  ✓ Handles rate limits (429) - waits and retries automatically
  ✓ Handles server errors (5xx) - waits and retries automatically
  ✓ Handles network errors - waits and retries automatically
  ✓ ${CONFIG.PAUSE_DURATION/1000}s pause every ${CONFIG.PAUSE_EVERY} profiles to respect API
  ✓ Global error handlers prevent any crash

OUTPUT FILES (in results/enriched_investors/):
  enriched_team_members.json    - Full enriched profiles
  enriched_team_members.csv     - CSV export
  enrichment_summary.json       - Stats and summary

EXAMPLES:
  # Enrich first 10 profiles (for testing)
  node enrich_investors.js --limit 10

  # Full overnight enrichment (safe, won't crash!)
  nohup node enrich_investors.js > enrich.log 2>&1 &
  tail -f enrich.log

  # Resume after interruption
  node enrich_investors.js --resume

  # Preview without making API calls
  node enrich_investors.js --dry-run --limit 20
`);
}

// ============================================================================
// ContactOut API
// ============================================================================

function enrichLinkedInProfile(linkedinUrl, retryCount = 0) {
    return new Promise((resolve, reject) => {
        const encodedUrl = encodeURIComponent(linkedinUrl);
        const endpoint = `/v1/linkedin/enrich?profile=${encodedUrl}`;
        
        // Calculate delay with exponential backoff, capped at MAX_RETRY_DELAY
        const getRetryDelay = (count) => {
            const delay = CONFIG.INITIAL_RETRY_DELAY * Math.pow(2, Math.min(count, 6));
            return Math.min(delay, CONFIG.MAX_RETRY_DELAY);
        };
        
        const options = {
            hostname: CONFIG.API_BASE,
            path: endpoint,
            method: 'GET',
            headers: {
                'Content-Type': 'application/json',
                'Accept': 'application/json',
                'token': CONFIG.API_TOKEN,
            }
        };

        const req = https.request(options, (res) => {
            let data = '';
            res.on('data', chunk => data += chunk);
            res.on('end', () => {
                if (res.statusCode === 200) {
                    try {
                        const json = JSON.parse(data);
                        if (json.status_code === 200 && json.profile) {
                            resolve(json.profile);
                        } else {
                            resolve(null); // No data found for this profile
                        }
                    } catch (e) {
                        // JSON parse error - retry
                        const delay = getRetryDelay(retryCount);
                        console.log(`\n⚠️  JSON parse error. Retrying in ${delay/1000}s...`);
                        sleep(delay).then(() => resolve(enrichLinkedInProfile(linkedinUrl, retryCount + 1)));
                    }
                } else if (res.statusCode === 429) {
                    // Rate limited - ALWAYS retry with exponential backoff
                    const delay = getRetryDelay(retryCount);
                    console.log(`\n⏳ Rate limited (429). Waiting ${delay/1000}s... (retry #${retryCount + 1})`);
                    sleep(delay).then(() => resolve(enrichLinkedInProfile(linkedinUrl, retryCount + 1)));
                } else if (res.statusCode === 403) {
                    // API credits exhausted - wait longer and retry
                    const delay = Math.max(getRetryDelay(retryCount), 60000); // At least 1 minute
                    console.log(`\n⚠️  API credits issue (403). Waiting ${delay/1000}s... (retry #${retryCount + 1})`);
                    sleep(delay).then(() => resolve(enrichLinkedInProfile(linkedinUrl, retryCount + 1)));
                } else if (res.statusCode >= 500) {
                    // Server error - retry
                    const delay = getRetryDelay(retryCount);
                    console.log(`\n⚠️  Server error (${res.statusCode}). Retrying in ${delay/1000}s...`);
                    sleep(delay).then(() => resolve(enrichLinkedInProfile(linkedinUrl, retryCount + 1)));
                } else if (res.statusCode === 404 || res.statusCode === 400) {
                    // Profile not found or bad request - don't retry, just skip
                    resolve(null);
                } else {
                    // Other error - retry
                    const delay = getRetryDelay(retryCount);
                    console.log(`\n⚠️  HTTP ${res.statusCode}. Retrying in ${delay/1000}s...`);
                    sleep(delay).then(() => resolve(enrichLinkedInProfile(linkedinUrl, retryCount + 1)));
                }
            });
        });

        req.on('error', (error) => {
            // Network error - ALWAYS retry
            const delay = getRetryDelay(retryCount);
            console.log(`\n⚠️  Network error: ${error.message}. Retrying in ${delay/1000}s...`);
            sleep(delay).then(() => resolve(enrichLinkedInProfile(linkedinUrl, retryCount + 1)));
        });

        req.setTimeout(30000, () => {
            req.destroy();
            // Timeout - retry
            const delay = getRetryDelay(retryCount);
            console.log(`\n⚠️  Request timeout. Retrying in ${delay/1000}s...`);
            sleep(delay).then(() => resolve(enrichLinkedInProfile(linkedinUrl, retryCount + 1)));
        });

        req.end();
    });
}

// ============================================================================
// Data Processing
// ============================================================================

function extractEnrichedData(originalMember, contactOutProfile) {
    if (!contactOutProfile) {
        return {
            ...originalMember,
            enriched: false,
            enriched_at: null,
        };
    }

    return {
        // Original data (for database relationships)
        id: originalMember.id,
        investor_id: originalMember.investor_id,
        investor_slug: originalMember.investor_slug,
        
        // Basic info (prefer ContactOut data if available)
        name: contactOutProfile.full_name || originalMember.name,
        title: originalMember.title,
        headline: contactOutProfile.headline || '',
        
        // Contact info - THE MAIN VALUE!
        emails: contactOutProfile.email || [],
        personal_emails: contactOutProfile.personal_email || [],
        work_emails: contactOutProfile.work_email || [],
        phones: contactOutProfile.phone || [],
        
        // Social profiles
        linkedin_url: contactOutProfile.url || originalMember.linkedin_url,
        twitter: contactOutProfile.twitter || [],
        github: contactOutProfile.github || [],
        facebook: contactOutProfile.facebook || [],
        websites: contactOutProfile.website || [],
        
        // Professional details
        summary: contactOutProfile.summary || '',
        industry: contactOutProfile.industry || '',
        location: contactOutProfile.location || '',
        country: contactOutProfile.country || '',
        seniority: contactOutProfile.seniority || '',
        job_function: contactOutProfile.job_function || '',
        
        // Current company (FULL data from ContactOut)
        current_company: contactOutProfile.company ? {
            name: contactOutProfile.company.name || '',
            linkedin_url: contactOutProfile.company.url || '',
            linkedin_company_id: contactOutProfile.company.linkedin_company_id || null,
            domain: contactOutProfile.company.domain || '',
            email_domain: contactOutProfile.company.email_domain || '',
            website: contactOutProfile.company.website || '',
            overview: contactOutProfile.company.overview || '',
            type: contactOutProfile.company.type || '',
            size: contactOutProfile.company.size || null,
            country: contactOutProfile.company.country || '',
            revenue: contactOutProfile.company.revenue || null,
            founded_at: contactOutProfile.company.founded_at || null,
            industry: contactOutProfile.company.industry || '',
            headquarter: contactOutProfile.company.headquarter || '',
            logo_url: contactOutProfile.company.logo_url || '',
            specialties: contactOutProfile.company.specialties || [],
            locations: contactOutProfile.company.locations || [],
        } : null,
        
        // Experience history (FULL data)
        experience: (contactOutProfile.experience || []).map(exp => ({
            title: exp.title || '',
            company: exp.company_name || '',
            summary: exp.summary || '',
            locality: exp.locality || '',
            start_date_year: exp.start_date_year || null,
            start_date_month: exp.start_date_month || null,
            end_date_year: exp.end_date_year || null,
            end_date_month: exp.end_date_month || null,
            is_current: exp.is_current || false,
            linkedin_url: exp.linkedin_url || '',
            logo_url: exp.logo_url || '',
            domain: exp.domain || '',
        })),
        
        // Education (FULL data)
        education: (contactOutProfile.education || []).map(edu => ({
            school_name: edu.school_name || '',
            degree: edu.degree || '',
            field_of_study: edu.field_of_study || '',
            description: edu.description || '',
            start_date_year: edu.start_date_year || '',
            end_date_year: edu.end_date_year || '',
            url: edu.url || '',
        })),
        
        // Skills
        skills: contactOutProfile.skills || [],
        
        // Languages (FULL data)
        languages: (contactOutProfile.languages || []).map(l => ({
            name: l.name || l,
            proficiency: l.proficiency || '',
        })),
        
        // Certifications (FULL data)
        certifications: (contactOutProfile.certifications || []).map(c => ({
            name: c.name || '',
            authority: c.authority || '',
            license: c.license || '',
            start_date_year: c.start_date_year || null,
            start_date_month: c.start_date_month || null,
            end_date_year: c.end_date_year || null,
            end_date_month: c.end_date_month || null,
        })),
        
        // Publications
        publications: contactOutProfile.publications || [],
        
        // Projects
        projects: contactOutProfile.projects || [],
        
        // Profile metadata
        profile_picture_url: contactOutProfile.profile_picture_url || '',
        followers: contactOutProfile.followers || null,
        work_status: contactOutProfile.work_status || null,
        contactout_updated_at: contactOutProfile.updated_at || '',
        
        // Meta
        enriched: true,
        enriched_at: new Date().toISOString(),
        avatar_url: originalMember.avatar_url,
    };
}

// ============================================================================
// Progress Management
// ============================================================================

function loadProgress() {
    try {
        if (fs.existsSync(CONFIG.PROGRESS_FILE)) {
            return JSON.parse(fs.readFileSync(CONFIG.PROGRESS_FILE, 'utf8'));
        }
    } catch (e) {}
    return {
        processedIds: [],
        enrichedMembers: [],
        stats: {
            total: 0,
            enriched: 0,
            notFound: 0,
            noLinkedIn: 0,
        }
    };
}

function saveProgress(progress) {
    fs.writeFileSync(CONFIG.PROGRESS_FILE, JSON.stringify(progress, null, 2));
}

// ============================================================================
// Output Functions
// ============================================================================

function saveOutputs(progress) {
    // Ensure output directory exists
    if (!fs.existsSync(CONFIG.OUTPUT_DIR)) {
        fs.mkdirSync(CONFIG.OUTPUT_DIR, { recursive: true });
    }

    // Save enriched team members JSON
    fs.writeFileSync(
        path.join(CONFIG.OUTPUT_DIR, 'enriched_team_members.json'),
        JSON.stringify(progress.enrichedMembers, null, 2)
    );

    // Save CSV
    const csvHeaders = [
        'ID', 'Investor ID', 'Investor Slug', 'Name', 'Title', 'Headline',
        'Personal Emails', 'Work Emails', 'All Emails', 'Phones',
        'LinkedIn', 'Twitter', 'Location', 'Country',
        'Current Company', 'Industry', 'Skills',
        'Enriched', 'Enriched At'
    ];

    const csvRows = [csvHeaders.join(',')];
    for (const m of progress.enrichedMembers) {
        const row = [
            escapeCSV(m.id),
            escapeCSV(m.investor_id),
            escapeCSV(m.investor_slug),
            escapeCSV(m.name),
            escapeCSV(m.title),
            escapeCSV(m.headline),
            escapeCSV((m.personal_emails || []).join('; ')),
            escapeCSV((m.work_emails || []).join('; ')),
            escapeCSV((m.emails || []).join('; ')),
            escapeCSV((m.phones || []).join('; ')),
            escapeCSV(m.linkedin_url),
            escapeCSV((m.twitter || []).join('; ')),
            escapeCSV(m.location),
            escapeCSV(m.country),
            escapeCSV(m.current_company?.name || ''),
            escapeCSV(m.industry),
            escapeCSV((m.skills || []).slice(0, 10).join('; ')),
            escapeCSV(m.enriched),
            escapeCSV(m.enriched_at),
        ];
        csvRows.push(row.join(','));
    }
    fs.writeFileSync(
        path.join(CONFIG.OUTPUT_DIR, 'enriched_team_members.csv'),
        csvRows.join('\n')
    );

    // Save summary
    const summary = {
        generated_at: new Date().toISOString(),
        stats: progress.stats,
        emails_found: progress.enrichedMembers.filter(m => m.emails?.length > 0).length,
        phones_found: progress.enrichedMembers.filter(m => m.phones?.length > 0).length,
        personal_emails_found: progress.enrichedMembers.filter(m => m.personal_emails?.length > 0).length,
        work_emails_found: progress.enrichedMembers.filter(m => m.work_emails?.length > 0).length,
    };
    fs.writeFileSync(
        path.join(CONFIG.OUTPUT_DIR, 'enrichment_summary.json'),
        JSON.stringify(summary, null, 2)
    );

    console.log(`\n📁 Output saved to: ${CONFIG.OUTPUT_DIR}/`);
    console.log(`   ├── enriched_team_members.json`);
    console.log(`   ├── enriched_team_members.csv`);
    console.log(`   └── enrichment_summary.json`);
}

function escapeCSV(value) {
    if (value === null || value === undefined) return '';
    let str = String(value).replace(/[\r\n]+/g, ' ').trim();
    if (str.includes(',') || str.includes('"') || str.includes('\n')) {
        return `"${str.replace(/"/g, '""')}"`;
    }
    return str;
}

// ============================================================================
// Progress Display
// ============================================================================

function printProgress(current, total, startTime, name) {
    const percent = Math.min(100, Math.round((current / total) * 100));
    const elapsed = (Date.now() - startTime) / 1000;
    const rate = current / elapsed || 0;
    const remaining = rate > 0 ? Math.round((total - current) / rate) : 0;
    
    const barWidth = 25;
    const filled = Math.round((percent / 100) * barWidth);
    const bar = '█'.repeat(filled) + '░'.repeat(barWidth - filled);
    
    const eta = remaining > 0 ? `${Math.floor(remaining / 60)}m ${remaining % 60}s` : '--';
    const displayName = name ? name.substring(0, 25).padEnd(25) : '';
    
    process.stdout.write(`\r[${bar}] ${percent}% | ${current}/${total} | ETA: ${eta} | ${displayName}`);
}

// ============================================================================
// Main Enrichment Function
// ============================================================================

async function enrich(options) {
    console.log(`
╔══════════════════════════════════════════════════════════════════════════════╗
║              Investor Team Member Enrichment (ContactOut API)                ║
║                                                                              ║
║  📧 Fetching emails, phones, and full profiles for team members              ║
║  💰 This will use ContactOut API credits!                                    ║
╚══════════════════════════════════════════════════════════════════════════════╝
`);

    // Ensure output directory exists
    if (!fs.existsSync(CONFIG.OUTPUT_DIR)) {
        fs.mkdirSync(CONFIG.OUTPUT_DIR, { recursive: true });
    }

    // Load team members from scraped data
    const teamMembersFile = path.join(CONFIG.INPUT_DIR, 'team_members.json');
    const investorsFile = path.join(CONFIG.INPUT_DIR, 'investors.json');
    
    if (!fs.existsSync(teamMembersFile) || !fs.existsSync(investorsFile)) {
        console.error('❌ Data files not found. Run the scraper first.');
        process.exit(1);
    }

    const allTeamMembers = JSON.parse(fs.readFileSync(teamMembersFile, 'utf8'));
    const allInvestors = JSON.parse(fs.readFileSync(investorsFile, 'utf8'));
    
    console.log(`📊 Total team members in database: ${allTeamMembers.length}`);
    console.log(`📊 Total investors/funds in database: ${allInvestors.length}`);

    // Find US-based funds (check country field for "United States" or US cities)
    const usFunds = allInvestors.filter(inv => {
        const country = (inv.country || '').toLowerCase();
        const region = (inv.region || '').toLowerCase();
        
        // Check if country is United States
        if (country.includes('united states') || country === 'us' || country === 'usa') {
            return true;
        }
        
        // Check if region is a US state
        const usStates = ['california', 'new york', 'texas', 'florida', 'illinois', 'massachusetts', 
                         'washington', 'colorado', 'georgia', 'pennsylvania', 'ohio', 'michigan',
                         'north carolina', 'arizona', 'virginia', 'maryland', 'minnesota', 'oregon',
                         'utah', 'connecticut', 'tennessee', 'indiana', 'missouri', 'wisconsin',
                         'nevada', 'new jersey', 'delaware', 'district of columbia'];
        if (usStates.some(state => region.includes(state))) {
            return true;
        }
        
        return false;
    });
    
    const usFundSlugs = new Set(usFunds.map(f => f.slug));
    
    console.log(`🇺🇸 US-based funds: ${usFunds.length}`);

    // Filter team members to only those from US funds AND with LinkedIn URLs
    const membersWithLinkedIn = allTeamMembers.filter(m => 
        m.linkedin_url && usFundSlugs.has(m.investor_slug)
    );
    console.log(`🔗 US fund team members with LinkedIn: ${membersWithLinkedIn.length}`);

    // Load or initialize progress
    let progress = options.resume ? loadProgress() : {
        processedIds: [],
        enrichedMembers: [],
        stats: { total: 0, enriched: 0, notFound: 0, noLinkedIn: 0 }
    };

    if (options.resume && progress.processedIds.length > 0) {
        console.log(`📂 Resuming: ${progress.processedIds.length} already processed`);
    }

    // Determine which members to process
    const processedSet = new Set(progress.processedIds);
    const toProcess = membersWithLinkedIn
        .filter(m => !processedSet.has(m.id))
        .slice(0, options.limit);

    console.log(`🎯 Will enrich: ${toProcess.length} profiles`);
    console.log(`⏱️  Delay: ${options.delay}ms between requests`);

    if (options.dryRun) {
        console.log(`\n🔍 DRY RUN - Would enrich these profiles:\n`);
        toProcess.slice(0, 20).forEach((m, i) => {
            console.log(`  ${i + 1}. ${m.name} (${m.title}) - ${m.investor_slug}`);
            console.log(`     ${m.linkedin_url}`);
        });
        if (toProcess.length > 20) {
            console.log(`  ... and ${toProcess.length - 20} more`);
        }
        return;
    }

    if (toProcess.length === 0) {
        console.log('\n✅ All profiles already enriched!');
        return;
    }

    console.log(`\n🚀 Starting enrichment...\n`);
    console.log(`🛡️  Bulletproof mode: Will never stop on errors, infinite retries enabled\n`);

    const startTime = Date.now();
    let consecutiveInBatch = 0;
    let i = 0;

    while (i < toProcess.length) {
        const member = toProcess[i];
        
        try {
            printProgress(i + 1, toProcess.length, startTime, member.name);

            // Periodic pause
            if (consecutiveInBatch > 0 && consecutiveInBatch % CONFIG.PAUSE_EVERY === 0) {
                saveProgress(progress);
                console.log(`\n💾 Checkpoint: ${progress.stats.enriched} enriched, ${progress.stats.notFound} not found`);
                await sleep(CONFIG.PAUSE_DURATION);
                console.log('▶️  Resuming...\n');
            }

            // Call ContactOut API
            await sleep(options.delay);
            const enrichedProfile = await enrichLinkedInProfile(member.linkedin_url);

            // Process result
            const enrichedMember = extractEnrichedData(member, enrichedProfile);
            progress.enrichedMembers.push(enrichedMember);
            progress.processedIds.push(member.id);
            progress.stats.total++;

            if (enrichedProfile) {
                progress.stats.enriched++;
                if (enrichedProfile.email?.length > 0) {
                    // Show when we find emails
                    const emails = enrichedProfile.email.join(', ');
                    process.stdout.write(` ✓ ${emails.substring(0, 40)}`);
                }
            } else {
                progress.stats.notFound++;
            }

            consecutiveInBatch++;

            // Save progress periodically
            if ((i + 1) % 10 === 0) {
                saveProgress(progress);
            }
            
            // Move to next profile
            i++;
            
        } catch (error) {
            // Catch ANY error and continue - never crash
            console.error(`\n❌ Unexpected error on ${member.name}: ${error.message}`);
            console.log('💾 Saving progress and continuing...');
            saveProgress(progress);
            
            // Wait a bit before retrying this same profile
            await sleep(CONFIG.INITIAL_RETRY_DELAY);
            
            // Don't increment i - will retry this same profile
            console.log(`🔄 Retrying ${member.name}...`);
        }
    }

    console.log('\n');

    // Save final outputs and progress (keep progress for resume)
    saveProgress(progress);
    saveOutputs(progress);

    // Print summary
    const elapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
    const emailsFound = progress.enrichedMembers.filter(m => m.emails?.length > 0).length;
    const phonesFound = progress.enrichedMembers.filter(m => m.phones?.length > 0).length;

    console.log(`
╔══════════════════════════════════════════════════════════════════════════════╗
║                           ✅ Enrichment Complete!                            ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  Profiles processed:  ${String(progress.stats.total).padEnd(54)}║
║  Successfully enriched: ${String(progress.stats.enriched).padEnd(52)}║
║  Not found:           ${String(progress.stats.notFound).padEnd(54)}║
║                                                                              ║
║  📧 Emails found:      ${String(emailsFound).padEnd(53)}║
║  📱 Phones found:      ${String(phonesFound).padEnd(53)}║
║  ⏱️  Time elapsed:      ${(elapsed + ' minutes').padEnd(53)}║
╚══════════════════════════════════════════════════════════════════════════════╝

💡 To build unified database with enriched data, run:
   node build_unified_database.js
`);
}

// ============================================================================
// Entry Point
// ============================================================================

// Global error handlers to prevent crashes
process.on('uncaughtException', (error) => {
    console.error(`\n🚨 Uncaught exception: ${error.message}`);
    console.log('💾 Progress is saved. Restart with --resume to continue.');
    // Don't exit - try to continue
});

process.on('unhandledRejection', (reason, promise) => {
    console.error(`\n🚨 Unhandled rejection: ${reason}`);
    console.log('💾 Progress is saved. Restart with --resume to continue.');
    // Don't exit - try to continue
});

const options = parseArgs();
enrich(options).catch((error) => {
    console.error(`\n🚨 Fatal error: ${error.message}`);
    console.log('💾 Progress is saved. Restart with --resume to continue.');
    console.log('\nTo resume, run:');
    console.log('  node enrich_investors.js --resume');
});

