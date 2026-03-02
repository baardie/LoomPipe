import { useState } from 'react';
import {
  BookOpen, Database, ArrowRightLeft, Zap, Shield, Clock,
  Terminal, Layers, FileText, ChevronRight, Search, ExternalLink,
  Server, Globe, FileSpreadsheet, Cloud, BarChart3, Box,
  Key, Upload, Mail, RefreshCw, Play, Settings, Hash,
} from 'lucide-react';

/* ─── Helpers ─────────────────────────────────────────────────────────────── */

const Badge = ({ children, color = 'indigo' }) => {
  const colors = {
    indigo:  'bg-indigo-500/10 text-indigo-400 border-indigo-500/20',
    emerald: 'bg-emerald-500/10 text-emerald-400 border-emerald-500/20',
    amber:   'bg-amber-500/10 text-amber-400 border-amber-500/20',
    rose:    'bg-rose-500/10 text-rose-400 border-rose-500/20',
    purple:  'bg-purple-500/10 text-purple-400 border-purple-500/20',
    slate:   'bg-slate-500/10 text-slate-400 border-slate-500/20',
  };
  return (
    <span className={`inline-flex text-[10px] font-semibold px-2 py-0.5 rounded-full border ${colors[color]}`}>
      {children}
    </span>
  );
};

const Code = ({ children }) => (
  <code className="px-1.5 py-0.5 text-[11px] bg-[var(--bg-elevated)] border border-[var(--border)] rounded font-mono text-indigo-300">
    {children}
  </code>
);

const CodeBlock = ({ children, title }) => (
  <div className="rounded-lg border border-[var(--border)] overflow-hidden my-3">
    {title && (
      <div className="px-3 py-1.5 bg-[var(--bg-elevated)] border-b border-[var(--border)] text-[10px] font-semibold text-[var(--text-muted)] uppercase tracking-wider">
        {title}
      </div>
    )}
    <pre className="px-4 py-3 text-[11px] leading-relaxed font-mono text-slate-300 bg-[var(--bg-surface)] overflow-x-auto whitespace-pre">
      {children}
    </pre>
  </div>
);

const SectionHeader = ({ icon: Icon, title, description }) => (
  <div className="mb-6">
    <div className="flex items-center gap-2 mb-1">
      <Icon size={18} className="text-[var(--accent)]" />
      <h2 className="text-lg font-bold text-[var(--text-primary)]">{title}</h2>
    </div>
    {description && <p className="text-xs text-[var(--text-secondary)] ml-[26px]">{description}</p>}
  </div>
);

const Table = ({ headers, rows }) => (
  <div className="overflow-x-auto rounded-lg border border-[var(--border)] my-3">
    <table className="w-full text-xs">
      <thead>
        <tr className="bg-[var(--bg-elevated)]">
          {headers.map((h, i) => (
            <th key={i} className="text-left px-3 py-2 font-semibold text-[var(--text-secondary)] uppercase tracking-wider text-[10px] border-b border-[var(--border)]">
              {h}
            </th>
          ))}
        </tr>
      </thead>
      <tbody>
        {rows.map((row, ri) => (
          <tr key={ri} className="border-b border-[var(--border)] last:border-b-0 hover:bg-[var(--bg-elevated)]/50 transition-colors">
            {row.map((cell, ci) => (
              <td key={ci} className="px-3 py-2 text-[var(--text-primary)]">{cell}</td>
            ))}
          </tr>
        ))}
      </tbody>
    </table>
  </div>
);

/* ─── Sidebar / TOC ───────────────────────────────────────────────────────── */

const TOC_SECTIONS = [
  { key: 'getting-started',   label: 'Getting Started',       icon: Play },
  { key: 'connectors',        label: 'Connectors',            icon: Database },
  { key: 'transformations',   label: 'Transformations',       icon: ArrowRightLeft },
  { key: 'pipelines',         label: 'Pipeline Config',       icon: Layers },
  { key: 'api-reference',     label: 'API Reference',         icon: Terminal },
  { key: 'authentication',    label: 'Authentication',        icon: Shield },
  { key: 'deployment',        label: 'Deployment',            icon: Server },
  { key: 'architecture',      label: 'Architecture',          icon: Box },
];

/* ─── Content Sections ────────────────────────────────────────────────────── */

const GettingStartedSection = () => (
  <section>
    <SectionHeader icon={Play} title="Getting Started" description="Get LoomPipe running in under 5 minutes." />

    <div className="space-y-4">
      <div className="p-4 rounded-lg bg-[var(--bg-surface)] border border-[var(--border)]">
        <h3 className="text-sm font-bold text-[var(--text-primary)] mb-2 flex items-center gap-2">
          <Cloud size={14} className="text-indigo-400" /> Docker (Recommended)
        </h3>
        <p className="text-xs text-[var(--text-secondary)] mb-2">Single container, no external dependencies. Uses SQLite by default.</p>
        <CodeBlock>{`git clone https://github.com/baardie/LoomPipe.git
cd LoomPipe
docker compose up --build`}</CodeBlock>
        <p className="text-xs text-[var(--text-secondary)]">
          The app starts at <Code>http://localhost:8080</Code>. Check the server logs for the auto-generated admin password.
        </p>
      </div>

      <div className="p-4 rounded-lg bg-[var(--bg-surface)] border border-[var(--border)]">
        <h3 className="text-sm font-bold text-[var(--text-primary)] mb-2 flex items-center gap-2">
          <Terminal size={14} className="text-emerald-400" /> Local Development
        </h3>
        <p className="text-xs text-[var(--text-secondary)] mb-2">
          Requires <Code>.NET 10 SDK</Code> and <Code>Node.js 20+</Code>.
        </p>
        <CodeBlock title="Backend">{`dotnet run --project LoomPipe.Server
# API starts at http://localhost:5259`}</CodeBlock>
        <CodeBlock title="Frontend">{`cd loompipe.client && npm install
npm run dev
# UI at http://localhost:5173 (proxies API calls)`}</CodeBlock>
      </div>

      <div className="p-4 rounded-lg bg-[var(--bg-surface)] border border-[var(--border)]">
        <h3 className="text-sm font-bold text-[var(--text-primary)] mb-2 flex items-center gap-2">
          <Database size={14} className="text-amber-400" /> Database Providers
        </h3>
        <p className="text-xs text-[var(--text-secondary)] mb-3">
          LoomPipe supports three database providers. Set in <Code>appsettings.json</Code>:
        </p>
        <Table
          headers={['Provider', 'Config Value', 'Notes']}
          rows={[
            ['SQLite', '"Sqlite"', 'Zero-config default — no external DB needed'],
            ['SQL Server', '"SqlServer"', 'LocalDB or full SQL Server instance'],
            ['PostgreSQL', '"PostgreSQL"', 'Requires provider-specific migrations'],
          ]}
        />
        <CodeBlock title="appsettings.json">{`{
  "Database": { "Provider": "Sqlite" },
  "ConnectionStrings": {
    "DefaultConnection": "Data Source=loompipe.db"
  }
}`}</CodeBlock>
      </div>

      <div className="p-4 rounded-lg bg-indigo-500/5 border border-indigo-500/20">
        <h3 className="text-sm font-bold text-indigo-300 mb-1">Default Credentials</h3>
        <p className="text-xs text-[var(--text-secondary)]">
          On first startup, a default <Code>admin</Code> account is created with a randomly generated password logged to the console. Change this immediately in production.
        </p>
      </div>
    </div>
  </section>
);

const ConnectorsSection = () => (
  <section>
    <SectionHeader icon={Database} title="Connectors" description="130 source connectors and 22 destination writers — databases, SaaS platforms, cloud storage, analytics, marketing, and more." />

    <h3 className="text-sm font-bold text-[var(--text-primary)] mb-2">Source Connectors (130)</h3>
    <p className="text-xs text-[var(--text-secondary)] mb-3">Organized by category. All connectors support the searchable picker in the pipeline editor and connection profile forms.</p>
    <Table
      headers={['Category', 'Connectors', 'Count']}
      rows={[
        [<Badge color="emerald">Databases</Badge>, 'SQL Server, PostgreSQL, MySQL, Oracle, MongoDB, Neo4j, Snowflake, BigQuery, Pinecone, Milvus, Elasticsearch, DynamoDB, Redis, Cassandra, ClickHouse, Databricks, Redshift', '17'],
        [<Badge color="indigo">CRM & Sales</Badge>, 'Salesforce, HubSpot, Pipedrive, Zoho CRM, Dynamics 365, Copper, Close, Freshsales', '8'],
        [<Badge color="indigo">E-commerce</Badge>, 'Stripe, Shopify, WooCommerce, BigCommerce, Magento, Shopify Plus, SF Commerce Cloud, Square, PayPal', '9'],
        [<Badge color="amber">Marketing & Ads</Badge>, 'Google Ads, Facebook Ads, LinkedIn Ads, TikTok Ads, Bing Ads, Microsoft Ads, Pinterest Ads, Snapchat Ads', '8'],
        [<Badge color="amber">Analytics</Badge>, 'Google Analytics, Google Search Console, Mixpanel, Amplitude, Segment, Snowplow', '6'],
        [<Badge color="purple">Communication</Badge>, 'Slack, Microsoft Teams, Twilio, Intercom', '4'],
        [<Badge color="purple">Project & Dev</Badge>, 'Jira, GitHub, GitLab, Bitbucket, Asana, Monday.com, Linear, Notion, Confluence', '9'],
        [<Badge color="rose">Customer Support</Badge>, 'Zendesk, Freshdesk, ServiceNow', '3'],
        [<Badge color="rose">Marketing Automation</Badge>, 'Mailchimp, SendGrid, Klaviyo, Marketo, Pardot, Brevo, SF Marketing Cloud, Salesloft, Outreach, Apollo.io', '10'],
        [<Badge color="slate">Finance & Billing</Badge>, 'QuickBooks, Xero, Chargebee, Recurly, Zuora, NetSuite, Harvest, Toggl', '8'],
        [<Badge color="indigo">Cloud Storage</Badge>, 'Amazon S3, Google Cloud Storage, Azure Blob, SFTP, Google Drive, SharePoint, Google Sheets', '7'],
        [<Badge color="amber">Social Media</Badge>, 'Instagram, YouTube, Twitter/X, Reddit', '4'],
        [<Badge color="emerald">HR</Badge>, 'Workday, BambooHR, Gusto, Greenhouse, Lever', '5'],
        [<Badge color="purple">Productivity</Badge>, 'Airtable, Webflow, Typeform, SurveyMonkey', '4'],
        [<Badge color="rose">Monitoring</Badge>, 'Datadog, PagerDuty, Sentry', '3'],
        [<Badge color="slate">Enterprise</Badge>, 'SAP, Okta, Firebase', '3'],
        [<Badge color="slate">File Formats</Badge>, 'CSV, JSON, REST API, Webhook (dest only)', '4'],
      ]}
    />

    <h3 className="text-sm font-bold text-[var(--text-primary)] mb-2 mt-6">Destination Writers (22)</h3>
    <Table
      headers={['Connector', 'Type', 'Key Features']}
      rows={[
        [<><Badge color="indigo">webhook</Badge></>,         'HTTP POST',     'POST each record as JSON to any endpoint'],
        [<><Badge color="emerald">sqlserver</Badge></>,      'Relational DB', 'Batch insert with auto-table creation'],
        [<><Badge color="emerald">postgresql</Badge></>,     'Relational DB', 'Batch insert with auto-table creation'],
        [<><Badge color="emerald">mysql</Badge></>,          'Relational DB', 'Batch insert with auto-table creation'],
        [<><Badge color="emerald">oracle</Badge></>,         'Relational DB', 'Batch insert with auto-table creation'],
        [<><Badge color="purple">mongodb</Badge></>,         'Document DB',   'Collection insert (single or batch)'],
        [<><Badge color="purple">neo4j</Badge></>,           'Graph DB',      'Cypher MERGE/CREATE execution'],
        [<><Badge color="amber">snowflake</Badge></>,        'Cloud DW',      'Batch INSERT into target table'],
        [<><Badge color="amber">bigquery</Badge></>,         'Cloud DW',      'Streaming insert into dataset/table'],
        [<><Badge color="rose">pinecone</Badge></>,          'Vector DB',     'Upsert vectors with metadata'],
        [<><Badge color="rose">milvus</Badge></>,            'Vector DB',     'REST-based vector insert'],
        [<><Badge color="indigo">shopify</Badge></>,         'SaaS API',      'Create records via Admin REST API'],
        [<><Badge color="indigo">googlesheets</Badge></>,    'SaaS API',      'Append rows to spreadsheet'],
        [<><Badge color="amber">s3</Badge></>,               'Cloud Storage', 'Write CSV/JSON/JSONL to S3 or MinIO'],
        [<><Badge color="amber">gcs</Badge></>,              'Cloud Storage', 'Write to Google Cloud Storage buckets'],
        [<><Badge color="amber">azureblob</Badge></>,        'Cloud Storage', 'Write to Azure Blob containers'],
        [<><Badge color="amber">sftp</Badge></>,             'File Transfer', 'Upload files via SSH'],
        [<><Badge color="purple">slack</Badge></>,           'Messaging',     'Post messages to Slack channels'],
        [<><Badge color="purple">teams</Badge></>,           'Messaging',     'Post messages to Teams channels'],
        [<><Badge color="indigo">airtable</Badge></>,        'SaaS API',      'Create/update Airtable records'],
        [<><Badge color="indigo">woocommerce</Badge></>,     'E-commerce',    'Create products/orders via REST API'],
        [<><Badge color="indigo">bigcommerce</Badge></>,     'E-commerce',    'Create records via REST API'],
      ]}
    />

    <div className="mt-6 p-4 rounded-lg bg-[var(--bg-surface)] border border-[var(--border)]">
      <h3 className="text-sm font-bold text-[var(--text-primary)] mb-2">Multi-Resource Connectors</h3>
      <p className="text-xs text-[var(--text-secondary)] mb-3">
        SaaS connectors (Shopify, Stripe, HubSpot, and 80+ more) support multiple resources within a single connector.
        Select the specific endpoint via the <Code>Resource</Code> parameter in the connection config.
      </p>
      <Table
        headers={['Connector', 'Available Resources']}
        rows={[
          ['Shopify', 'orders, products, customers, inventory_items, collections, draft_orders, abandoned_checkouts, locations, pages, blogs, articles, metafields, price_rules, discount_codes, smart_collections, custom_collections'],
          ['Stripe', 'customers, charges, invoices, subscriptions, payment_intents, products, prices, balance_transactions, refunds, disputes, coupons, plans, checkout_sessions, payment_methods, payouts, events, transfer'],
          ['HubSpot', 'contacts, companies, deals, tickets, products, line_items, quotes, calls, emails, meetings, notes, tasks, owners, pipelines, stages'],
        ]}
      />
    </div>

    <div className="mt-4 p-4 rounded-lg bg-[var(--bg-surface)] border border-[var(--border)]">
      <h3 className="text-sm font-bold text-[var(--text-primary)] mb-2">Connection Parameters</h3>
      <p className="text-xs text-[var(--text-secondary)] mb-3">
        Each connector accepts a connection string plus optional <Code>Parameters</Code> JSON for advanced config:
      </p>
      <CodeBlock title="Example: Shopify Parameters">{`{
  "resource": "orders",
  "apiVersion": "2024-10",
  "accessToken": "shpat_xxxxx"
}`}</CodeBlock>
      <CodeBlock title="Example: S3 Parameters">{`{
  "bucket": "my-data-lake",
  "prefix": "exports/2024/",
  "format": "csv",
  "accessKeyId": "AKIA...",
  "secretAccessKey": "...",
  "region": "us-east-1"
}`}</CodeBlock>
    </div>
  </section>
);

const TransformationsSection = () => (
  <section>
    <SectionHeader icon={ArrowRightLeft} title="Transformation Expression Language" description="48 built-in functions across 7 categories. Expressions are defined one per line in the pipeline editor." />

    <div className="p-4 rounded-lg bg-[var(--bg-surface)] border border-[var(--border)] mb-4">
      <h3 className="text-sm font-bold text-[var(--text-primary)] mb-2">Expression Syntax</h3>
      <Table
        headers={['Syntax', 'Example', 'Result']}
        rows={[
          ['Literal assignment', <Code>Country = 'US'</Code>, 'Sets field to constant value'],
          ['Field copy', <Code>FullName = Name</Code>, 'Copies source field value'],
          ['Concatenation', <Code>Display = First + \' \' + Last</Code>, 'Joins values with operator'],
          ['Function call', <Code>Email = LOWER(Email)</Code>, 'Applies a built-in function'],
          ['Nested functions', <Code>Name = TRIM(UPPER(Name))</Code>, 'Composes multiple functions'],
        ]}
      />
    </div>

    <h3 className="text-sm font-bold text-[var(--text-primary)] mb-2 flex items-center gap-2">
      <Hash size={14} className="text-indigo-400" /> String Functions <Badge>20</Badge>
    </h3>
    <Table
      headers={['Function', 'Syntax', 'Description']}
      rows={[
        [<Code>UPPER</Code>,         'UPPER(field)',                    'Convert to uppercase'],
        [<Code>LOWER</Code>,         'LOWER(field)',                    'Convert to lowercase'],
        [<Code>TRIM</Code>,          'TRIM(field)',                     'Remove leading & trailing whitespace'],
        [<Code>LTRIM</Code>,         'LTRIM(field)',                    'Remove leading whitespace'],
        [<Code>RTRIM</Code>,         'RTRIM(field)',                    'Remove trailing whitespace'],
        [<Code>REPLACE</Code>,       'REPLACE(field, old, new)',        'Replace all occurrences of substring'],
        [<Code>REGEX_REPLACE</Code>, 'REGEX_REPLACE(field, pattern, replacement)', 'Replace using regex pattern'],
        [<Code>REVERSE</Code>,       'REVERSE(field)',                  'Reverse character order'],
        [<Code>LEFT</Code>,          'LEFT(field, n)',                  'Extract first N characters'],
        [<Code>RIGHT</Code>,         'RIGHT(field, n)',                 'Extract last N characters'],
        [<Code>SUBSTRING</Code>,     'SUBSTRING(field, start, len)',    'Extract substring (1-indexed)'],
        [<Code>LEN</Code>,           'LEN(field)',                      'Character count (also LENGTH)'],
        [<Code>PAD_LEFT</Code>,      'PAD_LEFT(field, width, char)',    'Left-pad to width'],
        [<Code>PAD_RIGHT</Code>,     'PAD_RIGHT(field, width, char)',   'Right-pad to width'],
        [<Code>SPLIT</Code>,         'SPLIT(field, delim, index)',      'Split and return part (1-indexed)'],
        [<Code>NORMALIZE</Code>,     'NORMALIZE(field)',                'Collapse whitespace, trim'],
        [<Code>TITLE_CASE</Code>,    'TITLE_CASE(field)',               'Convert To Title Case'],
        [<Code>SLUG</Code>,          'SLUG(field)',                     'URL-safe slug (lowercase, hyphens)'],
        [<Code>CONCAT</Code>,        'CONCAT(a, b, ...)',              'Concatenate 2+ fields'],
      ]}
    />

    <h3 className="text-sm font-bold text-[var(--text-primary)] mb-2 mt-6 flex items-center gap-2">
      <Hash size={14} className="text-emerald-400" /> Numeric Functions <Badge color="emerald">6</Badge>
    </h3>
    <Table
      headers={['Function', 'Syntax', 'Description']}
      rows={[
        [<Code>ROUND</Code>,   'ROUND(field, decimals)',  'Round to N decimal places'],
        [<Code>CEIL</Code>,    'CEIL(field)',             'Round up to nearest integer (also CEILING)'],
        [<Code>FLOOR</Code>,   'FLOOR(field)',            'Round down to nearest integer'],
        [<Code>ABS</Code>,     'ABS(field)',              'Absolute value'],
        [<Code>MOD</Code>,     'MOD(field, divisor)',     'Remainder of division'],
      ]}
    />

    <h3 className="text-sm font-bold text-[var(--text-primary)] mb-2 mt-6 flex items-center gap-2">
      <Hash size={14} className="text-amber-400" /> Type Conversion <Badge color="amber">4</Badge>
    </h3>
    <Table
      headers={['Function', 'Syntax', 'Description']}
      rows={[
        [<Code>TO_INT</Code>,    'TO_INT(field)',    'Parse as integer (0 on failure)'],
        [<Code>TO_FLOAT</Code>,  'TO_FLOAT(field)',  'Parse as double (0.0 on failure)'],
        [<Code>TO_STRING</Code>, 'TO_STRING(field)', 'Coerce to string representation'],
        [<Code>TO_BOOL</Code>,   'TO_BOOL(field)',   'Convert to boolean (true: "1"/"true"/"yes"/"on")'],
      ]}
    />

    <h3 className="text-sm font-bold text-[var(--text-primary)] mb-2 mt-6 flex items-center gap-2">
      <Hash size={14} className="text-purple-400" /> Date/Time Functions <Badge color="purple">8</Badge>
    </h3>
    <Table
      headers={['Function', 'Syntax', 'Description']}
      rows={[
        [<Code>NOW</Code>,         'NOW()',                         'Current UTC datetime (ISO-8601)'],
        [<Code>TODAY</Code>,       'TODAY()',                       'Current UTC date (yyyy-MM-dd)'],
        [<Code>FORMAT_DATE</Code>, 'FORMAT_DATE(field, format)',    'Reformat date with .NET format codes'],
        [<Code>ADD_DAYS</Code>,    'ADD_DAYS(field, n)',            'Add N days (supports decimals)'],
        [<Code>DATE_DIFF</Code>,   'DATE_DIFF(field1, field2)',     'Days between two dates (field2 - field1)'],
        [<Code>YEAR</Code>,       'YEAR(field)',                    'Extract year as integer'],
        [<Code>MONTH</Code>,      'MONTH(field)',                   'Extract month as integer'],
        [<Code>DAY</Code>,        'DAY(field)',                     'Extract day-of-month as integer'],
      ]}
    />

    <h3 className="text-sm font-bold text-[var(--text-primary)] mb-2 mt-6 flex items-center gap-2">
      <Hash size={14} className="text-rose-400" /> Null / Conditional <Badge color="rose">3</Badge>
    </h3>
    <Table
      headers={['Function', 'Syntax', 'Description']}
      rows={[
        [<Code>COALESCE</Code>, 'COALESCE(a, b, ...)',    'First non-null, non-empty value'],
        [<Code>DEFAULT</Code>,  'DEFAULT(field, fallback)', 'Return fallback if field is empty'],
        [<Code>NULLIF</Code>,   'NULLIF(field, value)',    'Return null if field equals value'],
      ]}
    />

    <h3 className="text-sm font-bold text-[var(--text-primary)] mb-2 mt-6 flex items-center gap-2">
      <Hash size={14} className="text-indigo-400" /> Encoding / Hashing <Badge>6</Badge>
    </h3>
    <Table
      headers={['Function', 'Syntax', 'Description']}
      rows={[
        [<Code>MD5</Code>,           'MD5(field)',           'MD5 hex digest (lowercase)'],
        [<Code>SHA256</Code>,        'SHA256(field)',        'SHA-256 hex digest (lowercase)'],
        [<Code>BASE64_ENCODE</Code>, 'BASE64_ENCODE(field)', 'Base-64 encode UTF-8 bytes'],
        [<Code>BASE64_DECODE</Code>, 'BASE64_DECODE(field)', 'Base-64 decode to UTF-8 string'],
        [<Code>URL_ENCODE</Code>,    'URL_ENCODE(field)',    'Percent-encode (RFC 3986)'],
        [<Code>URL_DECODE</Code>,    'URL_DECODE(field)',    'Percent-decode string'],
      ]}
    />
  </section>
);

const PipelinesSection = () => (
  <section>
    <SectionHeader icon={Layers} title="Pipeline Configuration" description="Pipelines connect a source to a destination with optional field mapping, transformations, scheduling, and batching." />

    <div className="space-y-4">
      <div className="p-4 rounded-lg bg-[var(--bg-surface)] border border-[var(--border)]">
        <h3 className="text-sm font-bold text-[var(--text-primary)] mb-2 flex items-center gap-2">
          <ArrowRightLeft size={14} className="text-indigo-400" /> Automap Field Mapping
        </h3>
        <p className="text-xs text-[var(--text-secondary)]">
          LoomPipe uses exact and fuzzy (Levenshtein distance) matching to auto-map source fields to destination fields in one click.
          You can override any mapping manually or add transformation expressions.
        </p>
      </div>

      <div className="p-4 rounded-lg bg-[var(--bg-surface)] border border-[var(--border)]">
        <h3 className="text-sm font-bold text-[var(--text-primary)] mb-2 flex items-center gap-2">
          <Clock size={14} className="text-emerald-400" /> Scheduling
        </h3>
        <p className="text-xs text-[var(--text-secondary)] mb-2">
          Enable scheduling on any pipeline to run at fixed intervals. Set the interval in minutes and LoomPipe handles the rest.
        </p>
        <Table
          headers={['Setting', 'Description']}
          rows={[
            [<Code>ScheduleEnabled</Code>, 'Toggle scheduling on/off'],
            [<Code>ScheduleIntervalMinutes</Code>, 'Minutes between runs (e.g., 60 = hourly)'],
            [<Code>NextRunAt</Code>, 'Automatically calculated — next scheduled execution time'],
          ]}
        />
      </div>

      <div className="p-4 rounded-lg bg-[var(--bg-surface)] border border-[var(--border)]">
        <h3 className="text-sm font-bold text-[var(--text-primary)] mb-2 flex items-center gap-2">
          <RefreshCw size={14} className="text-amber-400" /> Incremental / Delta Loads
        </h3>
        <p className="text-xs text-[var(--text-secondary)] mb-2">
          Watermark-based incremental loading — only pull records modified since the last run.
        </p>
        <ol className="text-xs text-[var(--text-secondary)] space-y-1 ml-4 list-decimal">
          <li>Enable incremental load in the pipeline editor's bottom settings strip</li>
          <li>Set the watermark field (e.g., <Code>updated_at</Code>, <Code>modified_date</Code>)</li>
          <li>First run reads all records; subsequent runs only fetch <Code>WHERE field {'>'} last_watermark</Code></li>
        </ol>
        <p className="text-xs text-[var(--text-muted)] mt-2">
          Supported: all relational DB connectors (SQL Server, PostgreSQL, MySQL, Oracle), Shopify, Stripe, HubSpot.
        </p>
      </div>

      <div className="p-4 rounded-lg bg-[var(--bg-surface)] border border-[var(--border)]">
        <h3 className="text-sm font-bold text-[var(--text-primary)] mb-2 flex items-center gap-2">
          <Layers size={14} className="text-purple-400" /> Batch Writing
        </h3>
        <p className="text-xs text-[var(--text-secondary)]">
          Configure <Code>BatchSize</Code> and <Code>BatchDelaySeconds</Code> to control throughput.
          Records are grouped into batches and written with an optional delay between each batch — useful for rate-limited APIs or high-volume destinations.
        </p>
      </div>

      <div className="p-4 rounded-lg bg-[var(--bg-surface)] border border-[var(--border)]">
        <h3 className="text-sm font-bold text-[var(--text-primary)] mb-2 flex items-center gap-2">
          <Play size={14} className="text-rose-400" /> Dry Run
        </h3>
        <p className="text-xs text-[var(--text-secondary)]">
          Preview the first N rows (default: 10) of any pipeline before committing a full run. The dry run reads from the source, applies all mappings and transformations, and shows you the output without writing to the destination.
        </p>
      </div>

      <div className="p-4 rounded-lg bg-[var(--bg-surface)] border border-[var(--border)]">
        <h3 className="text-sm font-bold text-[var(--text-primary)] mb-2 flex items-center gap-2">
          <RefreshCw size={14} className="text-indigo-400" /> Failed Run Retry
        </h3>
        <p className="text-xs text-[var(--text-secondary)]">
          Re-run any failed pipeline run with one click. LoomPipe stores a configuration snapshot at run time (retained for a configurable window, default 7 days) and replays with the original config. Falls back to current config if the snapshot has expired.
        </p>
      </div>
    </div>
  </section>
);

const ApiReferenceSection = () => (
  <section>
    <SectionHeader icon={Terminal} title="API Reference" description="All endpoints are accessible via JWT Bearer token or X-Api-Key header." />

    <h3 className="text-sm font-bold text-[var(--text-primary)] mb-2">Pipelines</h3>
    <Table
      headers={['Method', 'Endpoint', 'Description', 'Auth']}
      rows={[
        [<Badge color="emerald">GET</Badge>,  '/api/pipelines',                  'List all pipelines',            'Any role'],
        [<Badge color="emerald">GET</Badge>,  '/api/pipelines/{id}',             'Get pipeline by ID',            'Any role'],
        [<Badge color="indigo">POST</Badge>,  '/api/pipelines',                  'Create new pipeline',           'Admin'],
        [<Badge color="amber">PUT</Badge>,    '/api/pipelines/{id}',             'Update pipeline',               'Admin'],
        [<Badge color="rose">DELETE</Badge>,   '/api/pipelines/{id}',             'Delete pipeline',               'Admin'],
        [<Badge color="indigo">POST</Badge>,  '/api/pipelines/{id}/run',         'Trigger pipeline run',          'Admin, User'],
        [<Badge color="indigo">POST</Badge>,  '/api/pipelines/{id}/dry-run',     'Preview pipeline output',       'Admin, User'],
        [<Badge color="emerald">GET</Badge>,  '/api/pipelines/{id}/runs',        'Get run history',               'Admin, User'],
        [<Badge color="emerald">GET</Badge>,  '/api/pipelines/{id}/stats',       'Get pipeline statistics',       'Admin, User'],
        [<Badge color="indigo">POST</Badge>,  '/api/pipelines/runs/{runId}/retry', 'Retry a failed run',          'Admin, User'],
      ]}
    />

    <h3 className="text-sm font-bold text-[var(--text-primary)] mb-2 mt-6">Connection Profiles</h3>
    <Table
      headers={['Method', 'Endpoint', 'Description', 'Auth']}
      rows={[
        [<Badge color="emerald">GET</Badge>,  '/api/connections',                'List connection profiles',      'Admin (all), User (assigned)'],
        [<Badge color="emerald">GET</Badge>,  '/api/connections/{id}',           'Get profile by ID',             'Admin (all), User (assigned)'],
        [<Badge color="indigo">POST</Badge>,  '/api/connections',                'Create connection profile',     'Admin'],
        [<Badge color="amber">PUT</Badge>,    '/api/connections/{id}',           'Update connection profile',     'Admin'],
        [<Badge color="rose">DELETE</Badge>,   '/api/connections/{id}',           'Delete connection profile',     'Admin'],
        [<Badge color="indigo">POST</Badge>,  '/api/connections/test',           'Test connection',               'Admin'],
      ]}
    />

    <h3 className="text-sm font-bold text-[var(--text-primary)] mb-2 mt-6">Schema Discovery</h3>
    <Table
      headers={['Method', 'Endpoint', 'Description', 'Auth']}
      rows={[
        [<Badge color="indigo">POST</Badge>,  '/api/schema/source',             'Discover source schema (fields)', 'Admin'],
        [<Badge color="indigo">POST</Badge>,  '/api/schema/destination',        'Discover destination schema',     'Admin'],
        [<Badge color="indigo">POST</Badge>,  '/api/schema/resources',          'List available resources',        'Admin'],
      ]}
    />

    <h3 className="text-sm font-bold text-[var(--text-primary)] mb-2 mt-6">Authentication & Users</h3>
    <Table
      headers={['Method', 'Endpoint', 'Description', 'Auth']}
      rows={[
        [<Badge color="indigo">POST</Badge>,  '/api/auth/login',               'Login (returns JWT)',             'Public'],
        [<Badge color="emerald">GET</Badge>,  '/api/auth/me',                  'Get current user info',           'Any role'],
        [<Badge color="emerald">GET</Badge>,  '/api/users',                    'List all users',                  'Admin'],
        [<Badge color="indigo">POST</Badge>,  '/api/users',                    'Create user',                     'Admin'],
        [<Badge color="amber">PUT</Badge>,    '/api/users/{id}',               'Update user',                     'Admin'],
        [<Badge color="rose">DELETE</Badge>,   '/api/users/{id}',               'Delete user',                     'Admin'],
      ]}
    />

    <h3 className="text-sm font-bold text-[var(--text-primary)] mb-2 mt-6">Other Endpoints</h3>
    <Table
      headers={['Method', 'Endpoint', 'Description', 'Auth']}
      rows={[
        [<Badge color="emerald">GET</Badge>,  '/api/analytics/summary',          'Dashboard summary stats',       'Admin, User'],
        [<Badge color="emerald">GET</Badge>,  '/api/analytics/runs-by-day?days=7', 'Runs per day chart data',     'Admin, User'],
        [<Badge color="emerald">GET</Badge>,  '/api/apikeys',                    'List API keys',                 'Admin, User'],
        [<Badge color="indigo">POST</Badge>,  '/api/apikeys',                    'Generate new API key',          'Admin, User'],
        [<Badge color="rose">DELETE</Badge>,   '/api/apikeys/{id}',               'Revoke API key',                'Admin, User'],
        [<Badge color="emerald">GET</Badge>,  '/api/notifications',              'Get notifications',             'Any role'],
        [<Badge color="indigo">POST</Badge>,  '/api/csv/upload',                 'Upload CSV file',               'Admin'],
        [<Badge color="indigo">POST</Badge>,  '/api/json/upload',                'Upload JSON file',              'Admin'],
      ]}
    />
  </section>
);

const AuthenticationSection = () => (
  <section>
    <SectionHeader icon={Shield} title="Authentication & Security" description="Dual-scheme auth with role-based access control, encrypted credential storage, and per-user connection permissions." />

    <div className="space-y-4">
      <div className="p-4 rounded-lg bg-[var(--bg-surface)] border border-[var(--border)]">
        <h3 className="text-sm font-bold text-[var(--text-primary)] mb-2">Auth Methods</h3>
        <Table
          headers={['Method', 'Header', 'Best For']}
          rows={[
            ['JWT Bearer', <Code>Authorization: Bearer {'<token>'}</Code>, 'Interactive UI sessions'],
            ['API Key', <Code>X-Api-Key: {'<key>'}</Code>, 'CI/CD, scripts, programmatic triggers'],
          ]}
        />
        <CodeBlock title="Example: API Key usage">{`# Trigger a pipeline run from CI
curl -X POST https://your-instance/api/pipelines/42/run \\
     -H "X-Api-Key: <your-key>"

# List pipelines
curl https://your-instance/api/pipelines \\
     -H "X-Api-Key: <your-key>"`}</CodeBlock>
      </div>

      <div className="p-4 rounded-lg bg-[var(--bg-surface)] border border-[var(--border)]">
        <h3 className="text-sm font-bold text-[var(--text-primary)] mb-2">Role Matrix</h3>
        <Table
          headers={['Action', 'Admin', 'User', 'Guest']}
          rows={[
            ['View pipeline list',                         '✓', '✓', '✓'],
            ['View run history & analytics',               '✓', '✓', ''],
            ['Trigger a pipeline run',                     '✓', '✓', ''],
            ['View assigned connection profiles',          '✓', '✓', ''],
            ['Create / edit / delete pipelines',           '✓', '',  ''],
            ['Create / edit / delete connections',         '✓', '',  ''],
            ['Manage users',                               '✓', '',  ''],
            ['Configure schedules, batch & incremental',   '✓', '',  ''],
            ['Assign connection profiles to users',        '✓', '',  ''],
            ['Configure email notifications',              '✓', '',  ''],
            ['Manage API keys (own keys)',                  '✓', '✓', ''],
          ]}
        />
      </div>

      <div className="p-4 rounded-lg bg-[var(--bg-surface)] border border-[var(--border)]">
        <h3 className="text-sm font-bold text-[var(--text-primary)] mb-2">Security Features</h3>
        <ul className="text-xs text-[var(--text-secondary)] space-y-2">
          <li className="flex items-start gap-2"><Shield size={12} className="text-emerald-400 mt-0.5 shrink-0" /> Passwords hashed with BCrypt (work factor 11)</li>
          <li className="flex items-start gap-2"><Key size={12} className="text-emerald-400 mt-0.5 shrink-0" /> API keys stored as SHA-256 hashes — shown once at creation, never recoverable</li>
          <li className="flex items-start gap-2"><Shield size={12} className="text-emerald-400 mt-0.5 shrink-0" /> Connection secrets encrypted with AES-256-CBC (ASP.NET Core Data Protection)</li>
          <li className="flex items-start gap-2"><Shield size={12} className="text-emerald-400 mt-0.5 shrink-0" /> Watermark queries use parameterized ADO.NET commands; column names validated with <Code>{'^[\\w.]+$'}</Code></li>
          <li className="flex items-start gap-2"><Shield size={12} className="text-emerald-400 mt-0.5 shrink-0" /> SMTP passwords stored server-side only — never returned to the browser</li>
          <li className="flex items-start gap-2"><Shield size={12} className="text-emerald-400 mt-0.5 shrink-0" /> Per-user connection permissions — Admins assign which profiles each user can access</li>
        </ul>
      </div>
    </div>
  </section>
);

const DeploymentSection = () => (
  <section>
    <SectionHeader icon={Server} title="Deployment" description="Docker-first deployment with zero-config SQLite, or connect to PostgreSQL / SQL Server for production." />

    <div className="space-y-4">
      <div className="p-4 rounded-lg bg-[var(--bg-surface)] border border-[var(--border)]">
        <h3 className="text-sm font-bold text-[var(--text-primary)] mb-2">Docker Compose</h3>
        <CodeBlock title="docker-compose.yml">{`services:
  loompipe:
    build: .
    ports:
      - "8080:8080"
    environment:
      - JWT_SECRET_KEY=your-secret-key-here
    volumes:
      - loompipe-data:/app/appdata
      - loompipe-dpkeys:/root/.aspnet/DataProtection-Keys

volumes:
  loompipe-data:
  loompipe-dpkeys:`}</CodeBlock>
        <p className="text-xs text-[var(--text-secondary)] mt-2">
          The Data Protection keys volume ensures encrypted connection secrets survive container restarts.
        </p>
      </div>

      <div className="p-4 rounded-lg bg-[var(--bg-surface)] border border-[var(--border)]">
        <h3 className="text-sm font-bold text-[var(--text-primary)] mb-2">Environment Variables</h3>
        <Table
          headers={['Variable', 'Description', 'Default']}
          rows={[
            [<Code>JWT_SECRET_KEY</Code>,          'JWT signing secret (min 32 chars)',   'Auto-generated if not set'],
            [<Code>Database__Provider</Code>,      'Database provider',                   'Sqlite'],
            [<Code>ConnectionStrings__DefaultConnection</Code>, 'Database connection string', 'Data Source=/app/appdata/loompipe.db'],
            [<Code>ASPNETCORE_URLS</Code>,          'Listening URL',                       'http://+:8080'],
          ]}
        />
      </div>

      <div className="p-4 rounded-lg bg-[var(--bg-surface)] border border-[var(--border)]">
        <h3 className="text-sm font-bold text-[var(--text-primary)] mb-2">Multi-Stage Docker Build</h3>
        <p className="text-xs text-[var(--text-secondary)]">
          The Dockerfile uses a three-stage build for minimal image size:
        </p>
        <ol className="text-xs text-[var(--text-secondary)] space-y-1 ml-4 list-decimal mt-2">
          <li><Code>node:22-alpine</Code> — Build React frontend (npm install + vite build)</li>
          <li><Code>dotnet/sdk:10.0</Code> — Restore, publish .NET backend</li>
          <li><Code>dotnet/aspnet:10.0</Code> — Final runtime image with frontend static files</li>
        </ol>
      </div>

      <div className="p-4 rounded-lg bg-[var(--bg-surface)] border border-[var(--border)]">
        <h3 className="text-sm font-bold text-[var(--text-primary)] mb-2">Email Notifications</h3>
        <p className="text-xs text-[var(--text-secondary)] mb-2">
          Configure SMTP from <strong>Settings &rarr; Email Notifications</strong> in the UI:
        </p>
        <Table
          headers={['Setting', 'Description']}
          rows={[
            ['SMTP Host / Port', 'Mail server address and port (e.g., smtp.gmail.com:587)'],
            ['SSL / TLS', 'Enable STARTTLS — recommended for ports 465 and 587'],
            ['Username / Password', 'SMTP credentials (password stored server-side only)'],
            ['From Address / Name', 'Sender displayed in emails'],
            ['Admin Email', 'Recipient for all pipeline event emails'],
            ['Notify on failure', 'Alert when any pipeline run fails'],
            ['Notify on success', 'Alert when a pipeline run succeeds'],
          ]}
        />
      </div>
    </div>
  </section>
);

const ArchitectureSection = () => (
  <section>
    <SectionHeader icon={Box} title="Architecture" description="Clean Architecture — dependencies flow inward, only inner layers are referenced by outer ones." />

    <CodeBlock title="Project Structure">{`LoomPipe/
├── LoomPipe.Core/            ← Entities, interfaces, DTOs (no external deps)
│       ↑
├── LoomPipe.Engine/          ← Pipeline orchestration, automap, transformations
├── LoomPipe.Connectors/      ← 130 source readers + 22 destination writers
├── LoomPipe.Storage/         ← EF Core repositories, DbContext, migrations
├── LoomPipe.Services/        ← Application services (connections, email)
├── LoomPipe.Workers/         ← Background scheduler (ConnectorWorker)
│       ↑
├── LoomPipe.Server/          ← ASP.NET Core Web API, dual-scheme auth
│       ↑
├── loompipe.client/          ← React 19 + Vite + Tailwind CSS
│
└── tests/
    ├── LoomPipe.Engine.Tests/       82 tests
    ├── LoomPipe.Connectors.Tests/    4 tests
    ├── LoomPipe.Core.Tests/          1 test
    └── LoomPipe.Server.Tests/        6 tests  (93 total)`}</CodeBlock>

    <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mt-4">
      <div className="p-4 rounded-lg bg-[var(--bg-surface)] border border-[var(--border)]">
        <h3 className="text-sm font-bold text-[var(--text-primary)] mb-2">Backend Stack</h3>
        <ul className="text-xs text-[var(--text-secondary)] space-y-1.5">
          <li>.NET 10 / ASP.NET Core Web API</li>
          <li>Entity Framework Core 10 (SQLite / PostgreSQL / SQL Server)</li>
          <li>JWT Bearer + API Key dual authentication</li>
          <li>BCrypt password hashing</li>
          <li>ASP.NET Core Data Protection (AES-256-CBC)</li>
          <li>SMTP email notifications</li>
          <li>Background scheduler (ConnectorWorker)</li>
        </ul>
      </div>

      <div className="p-4 rounded-lg bg-[var(--bg-surface)] border border-[var(--border)]">
        <h3 className="text-sm font-bold text-[var(--text-primary)] mb-2">Frontend Stack</h3>
        <ul className="text-xs text-[var(--text-secondary)] space-y-1.5">
          <li>React 19 + Vite</li>
          <li>Tailwind CSS</li>
          <li>Lucide React icons</li>
          <li>JWT token stored in localStorage</li>
          <li>Role-based UI guards</li>
          <li>Real-time notification centre</li>
          <li>Visual pipeline editor (Loom Canvas)</li>
        </ul>
      </div>
    </div>
  </section>
);

/* ─── Main Page ───────────────────────────────────────────────────────────── */

const DocumentationPage = () => {
  const [activeSection, setActiveSection] = useState('getting-started');
  const [search, setSearch] = useState('');

  const filteredSections = search
    ? TOC_SECTIONS.filter(s => s.label.toLowerCase().includes(search.toLowerCase()))
    : TOC_SECTIONS;

  const renderSection = () => {
    switch (activeSection) {
      case 'getting-started':   return <GettingStartedSection />;
      case 'connectors':        return <ConnectorsSection />;
      case 'transformations':   return <TransformationsSection />;
      case 'pipelines':         return <PipelinesSection />;
      case 'api-reference':     return <ApiReferenceSection />;
      case 'authentication':    return <AuthenticationSection />;
      case 'deployment':        return <DeploymentSection />;
      case 'architecture':      return <ArchitectureSection />;
      default:                  return <GettingStartedSection />;
    }
  };

  return (
    <div className="flex h-full">
      {/* Left TOC sidebar */}
      <aside className="w-56 flex-shrink-0 border-r border-[var(--border)] bg-[var(--bg-surface)] p-4 overflow-y-auto">
        <div className="flex items-center gap-2 mb-4">
          <BookOpen size={16} className="text-[var(--accent)]" />
          <span className="text-sm font-bold text-[var(--text-primary)]">Documentation</span>
        </div>

        <div className="relative mb-4">
          <Search size={12} className="absolute left-2.5 top-1/2 -translate-y-1/2 text-[var(--text-muted)]" />
          <input
            type="text"
            placeholder="Search docs…"
            value={search}
            onChange={e => setSearch(e.target.value)}
            className="w-full pl-7 pr-3 py-1.5 text-[11px] rounded bg-[var(--bg-elevated)] border border-[var(--border)] text-[var(--text-primary)] placeholder-[var(--text-muted)] focus:outline-none focus:border-[var(--accent)] transition-colors"
          />
        </div>

        <nav className="space-y-0.5">
          {filteredSections.map(({ key, label, icon: Icon }) => (
            <button
              key={key}
              onClick={() => { setActiveSection(key); setSearch(''); }}
              className={`w-full flex items-center gap-2 px-3 py-2 rounded-lg text-left text-xs transition-all duration-150 ${
                activeSection === key
                  ? 'bg-indigo-600/15 text-indigo-400 font-semibold'
                  : 'text-[var(--text-secondary)] hover:bg-[var(--bg-elevated)] hover:text-[var(--text-primary)]'
              }`}
            >
              <Icon size={14} />
              <span>{label}</span>
              {activeSection === key && <ChevronRight size={12} className="ml-auto" />}
            </button>
          ))}
        </nav>

        <div className="mt-6 pt-4 border-t border-[var(--border)]">
          <a
            href="https://github.com/baardie/LoomPipe"
            target="_blank"
            rel="noopener noreferrer"
            className="flex items-center gap-2 text-[11px] text-[var(--text-muted)] hover:text-[var(--text-primary)] transition-colors"
          >
            <ExternalLink size={12} />
            View on GitHub
          </a>
        </div>
      </aside>

      {/* Main content */}
      <div className="flex-1 overflow-y-auto p-6 max-w-4xl">
        {renderSection()}
      </div>
    </div>
  );
};

export default DocumentationPage;
