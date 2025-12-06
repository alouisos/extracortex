#!/usr/bin/env node
/**
 * Build Unified Investor Database
 * 
 * Merges scraped data with enriched ContactOut data into a single coherent database.
 * 
 * Usage:
 *   node build_unified_database.js
 */

const fs = require('fs');
const path = require('path');

const PATHS = {
    // Input
    investors: 'results/visible_db_export/investors.json',
    teamMembers: 'results/visible_db_export/team_members.json',
    investments: 'results/visible_db_export/investments.json',
    enrichedMembers: 'results/enriched_investors/enriched_team_members.json',
    enrichmentProgress: 'results/enriched_investors/.enrichment_progress.json',
    
    // Output
    outputDir: 'results/unified_database',
};

function escapeCSV(value) {
    if (value === null || value === undefined) return '';
    if (Array.isArray(value)) value = value.join('; ');
    let str = String(value).replace(/[\r\n]+/g, ' ').trim();
    if (str.includes(',') || str.includes('"') || str.includes('\n')) {
        return `"${str.replace(/"/g, '""')}"`;
    }
    return str;
}

async function buildDatabase() {
    console.log(`
╔══════════════════════════════════════════════════════════════════════════════╗
║                      Build Unified Investor Database                         ║
╚══════════════════════════════════════════════════════════════════════════════╝
`);

    // Ensure output directory exists
    if (!fs.existsSync(PATHS.outputDir)) {
        fs.mkdirSync(PATHS.outputDir, { recursive: true });
    }

    // Load all data
    console.log('📂 Loading data...');
    const investors = JSON.parse(fs.readFileSync(PATHS.investors, 'utf8'));
    const teamMembers = JSON.parse(fs.readFileSync(PATHS.teamMembers, 'utf8'));
    const investments = JSON.parse(fs.readFileSync(PATHS.investments, 'utf8'));
    
    // Load enriched members - prefer progress file (has all data) over output file
    let enrichedMembers = [];
    if (fs.existsSync(PATHS.enrichmentProgress)) {
        const progress = JSON.parse(fs.readFileSync(PATHS.enrichmentProgress, 'utf8'));
        enrichedMembers = progress.enrichedMembers || [];
        console.log(`   (loaded from progress file)`);
    } else if (fs.existsSync(PATHS.enrichedMembers)) {
        enrichedMembers = JSON.parse(fs.readFileSync(PATHS.enrichedMembers, 'utf8'));
        console.log(`   (loaded from output file)`);
    }

    console.log(`   Investors: ${investors.length.toLocaleString()}`);
    console.log(`   Team Members: ${teamMembers.length.toLocaleString()}`);
    console.log(`   Investments: ${investments.length.toLocaleString()}`);
    console.log(`   Enriched Profiles: ${enrichedMembers.length.toLocaleString()}`);

    // Create lookup maps
    const enrichedMap = new Map(enrichedMembers.map(m => [m.id, m]));
    const investorMap = new Map(investors.map(i => [i.id, i]));

    // =========================================================================
    // 1. UNIFIED TEAM MEMBERS - Merge original + enriched data
    // =========================================================================
    console.log('\n🔄 Merging team member data...');
    
    const unifiedTeamMembers = teamMembers.map(member => {
        const enriched = enrichedMap.get(member.id);
        const investor = investorMap.get(member.investor_id);
        
        if (enriched && enriched.enriched) {
            // Merge enriched data with original
            return {
                // IDs and relationships
                id: member.id,
                investor_id: member.investor_id,
                investor_slug: member.investor_slug,
                investor_name: investor?.name || '',
                
                // Basic info
                name: enriched.name || member.name,
                title: member.title,
                headline: enriched.headline || '',
                
                // CONTACT INFO (from ContactOut)
                emails: enriched.emails || [],
                personal_emails: enriched.personal_emails || [],
                work_emails: enriched.work_emails || [],
                phones: enriched.phones || [],
                
                // Social profiles
                linkedin_url: member.linkedin_url,
                twitter: enriched.twitter || [],
                github: enriched.github || [],
                
                // Location
                location: enriched.location || '',
                country: enriched.country || '',
                
                // Professional
                industry: enriched.industry || '',
                current_company: enriched.current_company || null,
                experience: enriched.experience || [],
                education: enriched.education || [],
                skills: enriched.skills || [],
                
                // Meta
                enriched: true,
                enriched_at: enriched.enriched_at,
                avatar_url: member.avatar_url,
            };
        } else {
            // Not enriched - use original data
            return {
                id: member.id,
                investor_id: member.investor_id,
                investor_slug: member.investor_slug,
                investor_name: investor?.name || '',
                
                name: member.name,
                title: member.title,
                headline: '',
                
                emails: [],
                personal_emails: [],
                work_emails: [],
                phones: [],
                
                linkedin_url: member.linkedin_url,
                twitter: [],
                github: [],
                
                location: '',
                country: '',
                industry: '',
                current_company: null,
                experience: [],
                education: [],
                skills: [],
                
                enriched: false,
                enriched_at: null,
                avatar_url: member.avatar_url,
            };
        }
    });

    const enrichedCount = unifiedTeamMembers.filter(m => m.enriched).length;
    const withEmails = unifiedTeamMembers.filter(m => m.emails.length > 0).length;
    const withPhones = unifiedTeamMembers.filter(m => m.phones.length > 0).length;

    console.log(`   ✓ ${enrichedCount.toLocaleString()} profiles enriched with ContactOut`);
    console.log(`   ✓ ${withEmails.toLocaleString()} have email addresses`);
    console.log(`   ✓ ${withPhones.toLocaleString()} have phone numbers`);

    // =========================================================================
    // 2. SAVE UNIFIED TEAM MEMBERS
    // =========================================================================
    console.log('\n💾 Saving unified team members...');
    
    fs.writeFileSync(
        path.join(PATHS.outputDir, 'team_members.json'),
        JSON.stringify(unifiedTeamMembers, null, 2)
    );

    // CSV with contact info
    const teamCsvHeaders = [
        'ID', 'Investor ID', 'Investor Name', 'Name', 'Title', 'Headline',
        'All Emails', 'Personal Emails', 'Work Emails', 'Phones',
        'LinkedIn', 'Twitter', 'GitHub', 'Location', 'Country',
        'Current Company', 'Industry', 'Skills', 'Enriched'
    ];
    const teamCsvRows = [teamCsvHeaders.join(',')];
    for (const m of unifiedTeamMembers) {
        teamCsvRows.push([
            escapeCSV(m.id),
            escapeCSV(m.investor_id),
            escapeCSV(m.investor_name),
            escapeCSV(m.name),
            escapeCSV(m.title),
            escapeCSV(m.headline),
            escapeCSV(m.emails),
            escapeCSV(m.personal_emails),
            escapeCSV(m.work_emails),
            escapeCSV(m.phones),
            escapeCSV(m.linkedin_url),
            escapeCSV(m.twitter),
            escapeCSV(m.github),
            escapeCSV(m.location),
            escapeCSV(m.country),
            escapeCSV(m.current_company?.name),
            escapeCSV(m.industry),
            escapeCSV(m.skills?.slice(0, 10)),
            escapeCSV(m.enriched),
        ].join(','));
    }
    fs.writeFileSync(
        path.join(PATHS.outputDir, 'team_members.csv'),
        teamCsvRows.join('\n')
    );

    // =========================================================================
    // 3. COPY INVESTORS AND INVESTMENTS (unchanged)
    // =========================================================================
    console.log('💾 Copying investors and investments...');
    
    fs.writeFileSync(
        path.join(PATHS.outputDir, 'investors.json'),
        JSON.stringify(investors, null, 2)
    );
    
    fs.writeFileSync(
        path.join(PATHS.outputDir, 'investments.json'),
        JSON.stringify(investments, null, 2)
    );

    // =========================================================================
    // 4. CREATE FULL NESTED DATABASE
    // =========================================================================
    console.log('💾 Building full nested database...');
    
    const teamByInvestor = {};
    unifiedTeamMembers.forEach(m => {
        if (!teamByInvestor[m.investor_id]) teamByInvestor[m.investor_id] = [];
        teamByInvestor[m.investor_id].push(m);
    });

    const investmentsByInvestor = {};
    investments.forEach(i => {
        if (!investmentsByInvestor[i.investor_id]) investmentsByInvestor[i.investor_id] = [];
        investmentsByInvestor[i.investor_id].push(i);
    });

    const fullDatabase = {
        metadata: {
            generated_at: new Date().toISOString(),
            source: 'https://connect.visible.vc/investors',
            enrichment_source: 'ContactOut API',
            counts: {
                investors: investors.length,
                team_members: unifiedTeamMembers.length,
                team_members_enriched: enrichedCount,
                team_members_with_email: withEmails,
                team_members_with_phone: withPhones,
                investments: investments.length,
            }
        },
        investors: investors.map(inv => ({
            ...inv,
            team_members: teamByInvestor[inv.id] || [],
            investments: investmentsByInvestor[inv.id] || [],
        }))
    };

    fs.writeFileSync(
        path.join(PATHS.outputDir, 'full_database.json'),
        JSON.stringify(fullDatabase, null, 2)
    );

    // =========================================================================
    // 5. CREATE SQL SCHEMA
    // =========================================================================
    console.log('💾 Creating SQL schema...');
    
    const sqlSchema = `
-- Unified Investor Database Schema
-- Generated: ${new Date().toISOString()}
-- Sources: Visible.vc + ContactOut API

-- ============================================================================
-- INVESTORS TABLE
-- ============================================================================
CREATE TABLE IF NOT EXISTS investors (
    id VARCHAR(36) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    slug VARCHAR(255) UNIQUE,
    website VARCHAR(500),
    description TEXT,
    city VARCHAR(100),
    region VARCHAR(100),
    country VARCHAR(100),
    check_size_min DECIMAL(15,2),
    check_size_max DECIMAL(15,2),
    sweet_spot DECIMAL(15,2),
    fund_size DECIMAL(15,2),
    stages JSON,
    focus JSON,
    tags JSON,
    thesis TEXT,
    verified BOOLEAN DEFAULT FALSE,
    linkedin_url VARCHAR(500),
    twitter_url VARCHAR(500),
    profile_url VARCHAR(500),
    scraped_at TIMESTAMP
);

-- ============================================================================
-- TEAM MEMBERS TABLE (with ContactOut enrichment)
-- ============================================================================
CREATE TABLE IF NOT EXISTS team_members (
    id VARCHAR(36) PRIMARY KEY,
    investor_id VARCHAR(36) REFERENCES investors(id) ON DELETE CASCADE,
    investor_slug VARCHAR(255),
    
    -- Basic Info
    name VARCHAR(255) NOT NULL,
    title VARCHAR(255),
    headline VARCHAR(500),
    
    -- CONTACT INFO (from ContactOut)
    emails JSON,              -- All emails as array
    personal_emails JSON,     -- Personal emails
    work_emails JSON,         -- Work emails
    phones JSON,              -- Phone numbers
    
    -- Social Profiles
    linkedin_url VARCHAR(500),
    twitter JSON,
    github JSON,
    
    -- Location
    location VARCHAR(255),
    country VARCHAR(100),
    
    -- Professional
    industry VARCHAR(255),
    current_company JSON,
    experience JSON,
    education JSON,
    skills JSON,
    
    -- Meta
    enriched BOOLEAN DEFAULT FALSE,
    enriched_at TIMESTAMP,
    avatar_url VARCHAR(500)
);

-- ============================================================================
-- INVESTMENTS TABLE
-- ============================================================================
CREATE TABLE IF NOT EXISTS investments (
    id VARCHAR(36) PRIMARY KEY,
    investor_id VARCHAR(36) REFERENCES investors(id) ON DELETE CASCADE,
    investor_slug VARCHAR(255),
    company_name VARCHAR(255) NOT NULL,
    company_website VARCHAR(500),
    amount_raised DECIMAL(15,2),
    announced_date DATE,
    company_city VARCHAR(100),
    company_country VARCHAR(100),
    article_url VARCHAR(500),
    scraped_at TIMESTAMP
);

-- ============================================================================
-- INDEXES
-- ============================================================================
CREATE INDEX idx_team_investor ON team_members(investor_id);
CREATE INDEX idx_team_enriched ON team_members(enriched);
CREATE INDEX idx_team_country ON team_members(country);
CREATE INDEX idx_investment_investor ON investments(investor_id);
CREATE INDEX idx_investment_date ON investments(announced_date);
CREATE INDEX idx_investor_country ON investors(country);

-- ============================================================================
-- USEFUL QUERIES
-- ============================================================================

-- Get all team members with emails for a specific fund:
-- SELECT name, title, emails, personal_emails, work_emails 
-- FROM team_members 
-- WHERE investor_slug = 'andreessen-horowitz' AND enriched = true;

-- Get all US-based investors with their team contact info:
-- SELECT i.name as fund, t.name, t.title, t.emails, t.phones
-- FROM investors i
-- JOIN team_members t ON i.id = t.investor_id
-- WHERE i.country = 'United States' AND t.enriched = true;

-- Find investors by team member email domain:
-- SELECT DISTINCT i.name, i.website
-- FROM investors i
-- JOIN team_members t ON i.id = t.investor_id
-- WHERE JSON_CONTAINS(t.work_emails, '"@a16z.com"');
`;

    fs.writeFileSync(
        path.join(PATHS.outputDir, 'schema.sql'),
        sqlSchema
    );

    // =========================================================================
    // 6. SUMMARY
    // =========================================================================
    const outputFiles = fs.readdirSync(PATHS.outputDir);
    
    console.log(`
╔══════════════════════════════════════════════════════════════════════════════╗
║                        ✅ Unified Database Created!                          ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  Output: results/unified_database/                                           ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  📁 Files:                                                                   ║
║     • investors.json        - ${investors.length.toLocaleString().padEnd(6)} funds                              ║
║     • team_members.json     - ${unifiedTeamMembers.length.toLocaleString().padEnd(6)} people (${enrichedCount} enriched)            ║
║     • team_members.csv      - Spreadsheet export with emails                 ║
║     • investments.json      - ${investments.length.toLocaleString().padEnd(6)} deals                              ║
║     • full_database.json    - Complete nested structure                      ║
║     • schema.sql            - Database schema                                ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  📧 Contact Data:                                                            ║
║     • ${withEmails.toLocaleString().padEnd(6)} team members have email addresses                        ║
║     • ${withPhones.toLocaleString().padEnd(6)} team members have phone numbers                          ║
╚══════════════════════════════════════════════════════════════════════════════╝

Database structure:
  investors (1) ──┬── team_members (many) ── with emails, phones from ContactOut
                  └── investments (many)
`);
}

buildDatabase().catch(console.error);

