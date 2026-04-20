# Product Definition
Auto Pro is Auto.finance's institutional and white-label offering: the same core autopool infrastructure, packaged for institutions such as family offices, fintechs, custodians, exchanges, funds, and other professional investors.
Anchors today: autoUSD (treasury-plus stablecoin yield) and autoETH (staking-plus ETH yield).
The calibration target is to separate true Auto Pro fintech buyers, operators, and high-value discovery conversations from generic finance, crypto-prestige, advisory, or investor-branded profiles.

## Naming Policy
The rubric describes categories of companies and roles, never specific company names or people's names. Any rewrite that introduces proper-noun company names (e.g., any identifiable brand) or individual names must be rejected. Use only generic descriptors ("mega-cap diversified tech platform", "emerging-market digital bank", "retail crypto exchange", "global card network", "BaaS platform", "VP of partnerships at a payments processor", etc.).

## Experiment Scope
This rubric intentionally collapses the prior 5-dimension FT score into two axes only: Company Fit v2 and Role Fit v2. Every non-FO rule from the 5-dim rubric is preserved below but reorganized by bucket:
- Company Fit v2 (cap 70) absorbs all company-level signal: is the company a real Auto Pro buyer universe (fintech / payments / BaaS / treasury / institutional-crypto), plus channel-vs-buyer, emerging-market fit, service-provider penalty, and reachability / access to the account.
- Role Fit v2 (cap 30) absorbs all person-level signal: functional alignment to Auto Pro, allocator authority, operator-vs-investor separation, and seniority weighted by relevance.
- Fintech Score v2 = Company Fit v2 + Role Fit v2 (must sum to 100).
family_office_relevance is intentionally omitted from this experiment.

## Output
Return one JSON object with a top-level results array. Each result must include exactly these keys:
- member_id
- Company Fit v2
- Role Fit v2
Rules:
- member_id must exactly equal the input member_id string.
- Use only the allowed point values from Direct Point Maps.
- Return no prose, no markdown, no derived explanations.

## Core Rules
- Score against Auto Pro specifically, not generic crypto prestige, generic fintech prestige, or generic executive impressiveness.
- Current company and current role are the primary decision surface. Current company description and current position description matter more than older biography.
- The additional organization block is supporting evidence, not the main anchor, unless the current-company block is genuinely thin.
- Company-level fit is the gatekeeper. A weak company caps the entire profile regardless of biography.
- An impressive title at an irrelevant / mismatched company should not get a high Role Fit v2.
- A great company with a peripheral role should not get a top Role Fit v2.

### Company-side constraints (all affect Company Fit v2)
- Hard Constraint – Mega-Cap Tech & Consumer Wallets (buyer_fit_negative): Mega-cap diversified tech platforms and massive consumer digital wallets (the largest peer-to-peer and online checkout brands) are too big to be direct buyers, REGARDLESS of the candidate's specific division. Cap Company Fit v2 at 14.
- Hard Constraint – Old School FX & Direct Lenders (buyer_fit_negative): Traditional corporate FX brokerages, old-school trade finance banks, direct real estate lenders, mortgage providers, and generic consumer lending firms are not buyers. Cap Company Fit v2 at 14.
- Hard Constraint – Credit Infra & Scoring (channel_vs_buyer): E-commerce funding infrastructure, credit scoring platforms, loan origination software, and generic lending infrastructure are channels or irrelevant, not treasury deployers. Cap Company Fit v2 at 14.
- Hard Constraint – Non-Fintech & IT Services (company_mismatch): Non-fintech IT services, education platforms, mentorship programs, AI development firms, and candidates who are "no longer fintech" (e.g., moved to nightlife, events, or entertainment) are NOT buyers. Cap Company Fit v2 at 14.
- Hard Constraint – Pure Crypto & Retail (company_mismatch): Retail crypto exchanges and pure crypto-trading platforms without institutional stablecoin/treasury products are not buyers. Cap Company Fit v2 at 14.
- Rescue – Modern Remittances & Merchant Acquirers (channel_vs_buyer rescue): Modern digital cross-border remittance platforms, emerging-market FX fintechs, and global B2B merchant acquirers are highly relevant buyers. Score Company Fit v2 56–70.
- Rescue – Deep Fintech & Niche Platforms (company_mismatch rescue): "Credit as a service" platforms, B2B payment solutions, deep-fintech clubs, school payments platforms, rental float platforms, micro-credit providers, corporate benefits platforms, and specialized payments advisory firms are highly relevant. Score Company Fit v2 56–70.
- Rescue – Global Card Networks & Embedded Fintech (company_mismatch rescue): Major global card networks (the largest global credit card brands) and embedded-fintech platforms are top-tier buyers. Do not confuse them with massive consumer digital wallets. Score Company Fit v2 at 70.
- Rescue – Sabbatical & Independent Consultants (other): If a candidate is on a "purposeful sabbatical", "designing my next chapter", running a personal "lab", or is an independent principal consultant, evaluate their stated focus or immediate past experience. If it indicates credit as a service, payments, or deep fintech, score Company Fit v2 at 70. Do not penalize for being on sabbatical or a consultant.

### Role-side constraints (all affect Role Fit v2)
- Hard Constraint – Mentors & Generic Service Providers (service_provider): Mentors, generic project leaders, and client management directors at core banking vendors cap Role Fit v2 at 6.
- Hard Constraint – Investor-Only & CVC (allocator_mismatch): Pure VC partners and angel investors generally cap at 12. EXCEPT: Venture capital partners and advisors who specifically focus on FinTech investing should be rescued to Role Fit v2 24.
- Rescue – Active Hybrids (allocator_mismatch rescue): Active hybrid "operator, investor, advisor" profiles who hold executive or board roles at fintechs should be scored as operators, not pure investors. Score Role Fit v2 24–30.
- Rescue – Sabbatical / Past Role Rescue (other): For candidates on sabbatical or acting as independent principal consultants, score Role Fit v2 based on their most recent operating role or consulting focus (e.g., payments, credit as a service). Score Role Fit v2 24–30 when that past role was fintech-operator-grade.
- Rescue – Innovation, Design & Transfer Solutions (allocator_mismatch rescue): Directors of Innovation & Design, and VPs of Product Management for Transfer Solutions at global card networks are top-tier. Score Role Fit v2 at 30.
- Rescue – B2B Sales Leadership (allocator_mismatch rescue): SVPs and Vice Presidents of Sales at B2B payment solutions companies score Role Fit v2 at 30.
- Operator Allocator Floor: An operator at a relevant fintech (BaaS, payments, digital banking, custody, exchange, institutional crypto) who owns product, partnerships, treasury, or distribution decisions gets Role Fit v2 minimum 24.

## Direct Point Maps
The two FT dimensions sum to exactly 100. family_office_relevance is not part of this rubric.
FT score point maps (sum of caps = 100):
- Company Fit v2 = 14, 28, 42, 56, 70
- Role Fit v2 = 6, 12, 18, 24, 30

## Score Bands
- qualified = 75-100
- nearly_qualified = 50-74
- little_qualified = 25-49
- totally_unqualified = 0-24

## Dimension Guidance

### Company Fit v2
- 70: Current company is a BaaS platform, payment processor, digital banking platform, B2B payment network, credit-as-a-service platform, global card network, embedded-fintech platform, deep-fintech club, school payments platform, rental float platform, micro-credit provider, corporate benefits platform, specialized payments advisory firm, global B2B merchant acquirer, or modern digital cross-border remittance platform. Also: candidates on sabbatical or independent consultants focused on payments/credit.
- 56: Strongly relevant one step from top tier: institutional crypto with treasury / payments product, emerging-market fintech with credit / payments / treasury.
- 42: Directionally relevant but indirect: mixed-fit fintech, traditional bank with a specific digital / fintech mandate.
- 28: Weak but visible: crypto infrastructure (channel_vs_buyer), generic big bank without a digital mandate.
- 14: Generic service provider, education platform, mentorship program, AI development firm, non-fintech IT services, no longer fintech (nightlife/events), retail crypto exchange, core banking software, credit scoring platform, loan origination software, e-commerce funding infrastructure, direct real estate lender, traditional corporate FX brokerage, old-school trade finance bank, mega-cap diversified tech platform, massive consumer digital wallet.
Rules:
- Use the current-company block first. If the current company is missing, vague, or generic, do not inflate the score based on the person alone.
- If the current company looks like a generic consultancy, recruiter, advisory shop, law firm, AI company, non-fintech IT service, or credit scoring platform, keep Company Fit v2 at 14 unless hard evidence shows a real Auto Pro buying or distribution mandate.
- Pure crypto / DeFi without institutional products is 14, not 28. Crypto companies with institutional payments / stablecoin / treasury products are 56, not 14.
- Emerging-market fintechs with credit, payments, or treasury products, as well as "credit-as-a-service" platforms, are 56–70, not 14.
- Channel vs buyer: BaaS / payments channel profiles are buyers at 56–70. Generic crypto-infra / security channels cap at 28. Core banking software, e-commerce funding infra, and credit infra cap at 14.
- Access is folded in as a modifier at the top two bands: without warmth evidence, do not push a 42-tier company above 56 based on reachability alone.

### Role Fit v2
- 30: Current role is directly aligned with treasury, product, B2B payments, credit-as-a-service, digital-assets infrastructure, partnerships, institutional distribution, innovation & design, or transfer solutions. SVPs / VPs of Sales at B2B payment companies. Also: candidates on sabbatical whose immediate past role was a top-tier fintech founder / executive. Active hybrid "operator, investor, advisor" profiles.
- 24: Strong role relevance with clear operating authority: operator / executive at a BaaS / payments / digital banking company who owns product, partnerships, or distribution decisions. FinTech-focused VC partners and advisors.
- 18: Moderate role relevance, or strong role at a weaker / mismatched company. Partial influence, crypto-infrastructure operator, bank executive on adjacent functions.
- 12: Weak role relevance or impressive-but-non-buyer title: generic VC partner / angel / board-only advisor without operating role, corporate-venture-capital / venture-banking head, VP at a mega-cap diversified tech platform or massive consumer digital wallet without digital-assets charter, crypto-infra sales, investor-only profile.
- 6: Role is mostly irrelevant to the likely buyer or adoption motion. Mentors, generic project leaders, client management directors at core banking vendors, generic service providers, recruiters, and pure advisors cap here.
Rules:
- Use current title and current position description first.
- Functional alignment beats title seniority. A VP / Director of partnerships, product, or treasury at a BaaS / payments company is 30, not 24 or 18.
- An impressive title at an irrelevant / mismatched company caps at 12.
- Mega-cap tech and massive consumer digital wallet roles cap at 12 regardless of title.
- Investor-only / advisor-only profiles cap at 12. Operators at relevant fintechs floor at 24. Active hybrid "operator, investor, advisor" profiles score 24–30.
- Generic service providers and pure advisors cap at 6. Specialized BaaS / payments consultants can score up to 24.

## Calibration Notes
- Optimize for separating Sent from Skip on the two-axis FT score.
- Cust is not a valid outcome for this experiment; the binary is SKIP vs GOOD only.
- This rubric starts at caps Company Fit v2 70 / Role Fit v2 30 on the hypothesis that current-company-fit is the dominant signal and role is a multiplier. The autopilot may rebalance within the ±6 weight step per iteration so long as caps continue to sum to exactly 100.
- All six v7 systematic-error targets are preserved, just relocated:
- False negatives matter most when real fintech operators, BaaS / payments leaders, treasury owners, infrastructure leaders, or strong channel-adjacent leads fall below 75. The explicit rescues above directly target these.
- False positives matter most when generic senior profiles, service providers, advisors, pure crypto / DeFi, mega-cap tech, massive consumer digital wallets, direct lenders, or investor-only profiles land above 75. The caps above directly target these.
