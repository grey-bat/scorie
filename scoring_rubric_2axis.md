# Lead Scoring Rubric — 2-Axis Experiment (Role + Company)

## Product Definition

Auto Pro is Auto.finance's institutional and white-label offering: the same core autopool infrastructure, packaged for institutions such as family offices, fintechs, custodians, exchanges, funds, and other professional investors.

Anchors today: `autoUSD` (treasury-plus stablecoin yield) and `autoETH` (staking-plus ETH yield).

The calibration target is to separate true Auto Pro fintech buyers, operators, and high-value discovery conversations from generic finance, crypto-prestige, advisory, or investor-branded profiles.

## Experiment Scope

This rubric intentionally collapses the prior 5-dimension FT score into **two axes only**: `company_fit` and `role_fit`. Every non-FO rule from the 5-dim rubric is preserved below but reorganized by bucket:

* `company_fit` (cap 60) absorbs all **company-level** signal: is the company a real Auto Pro buyer universe (fintech/payments/BaaS/treasury/institutional-crypto), plus channel-vs-buyer, emerging-market fit, service-provider penalty, and reachability/access to the account.
* `role_fit` (cap 40) absorbs all **person-level** signal: functional alignment to Auto Pro, allocator authority, operator-vs-investor separation, and seniority weighted by relevance.

`family_office_relevance` is intentionally omitted from this experiment.

## Output

Return one JSON object with a top-level `results` array. Each result must include exactly these keys:

* `member_id`
* `company_fit`
* `role_fit`

Rules:

* `member_id` must exactly equal the input `member_id` string.
* Use only the allowed point values from `Direct Point Maps`.
* Return no prose, no markdown, no derived explanations.

## Core Rules

* Score against Auto Pro specifically, not generic crypto prestige, generic fintech prestige, or generic executive impressiveness.

* Current company and current role are the primary decision surface. Current company description and current position description matter more than older biography.

* The additional organization block is supporting evidence, not the main anchor, unless the current-company block is genuinely thin.

* Company-level fit is the gatekeeper. A weak company caps the entire profile regardless of biography.

* An impressive title at an irrelevant/mismatched company should not get a high role_fit.

* A great company with a peripheral role should not get a top role_fit.

### Company-side constraints (all affect company_fit)

* **Hard Constraint – Retail Crypto Exchanges & Pure Crypto (company_mismatch):** Retail crypto exchanges (e.g., Mercado Bitcoin, Foxbit) and pure DeFi without a dedicated institutional treasury product are NOT buyers. Cap `company_fit` at 24. Do not score them as institutional custody.

* **Hard Constraint – Credit Infra / Core Banking (channel_vs_buyer):** Core banking software (e.g., Pismo) and lending infrastructure / credit infra (e.g., 8fig) are channels or software vendors, not treasury deployers. Cap `company_fit` at 24.

* **Hard Constraint – Credit Scoring (company_mismatch):** Credit scoring platforms (e.g., Base39) are not Auto Pro buyers. Cap `company_fit` at 24.

* **Hard Constraint – Traditional Banks & FX (buyer_fit_negative):** Traditional banks, FX/remittance firms (e.g., Ebury Bank), and massive diversified banks without a specific digital-assets / treasury / fintech mandate are too broad. Cap `company_fit` at 24.

* **Hard Constraint – Sabbatical / Past Experience Rescue (company_mismatch rescue):** If a candidate is currently "on sabbatical", "designing my next chapter", or "founder" of a new stealth/advisory firm (e.g., Next Chapter Lab, M10 Club), but their immediate past experience is top-tier fintech (e.g., PicPay, Guiabolso, Mercado Livre credit), score `company_fit` 48-60 based on their past company. Do not penalize for being on sabbatical.

* **Hard Constraint – Embedded Fintech & Payments (company_mismatch rescue):** Major payment networks (Visa, Mastercard, Worldpay) and embedded fintech platforms are highly relevant when the role is innovation, design, or partnerships. Score `company_fit` 48-60.

* **Hard Constraint – BaaS / Payments / Digital Banking (customize_not_skip):** Banking-as-a-Service platforms, payment processors, digital banking platforms, B2B payment networks, and custody / exchange platforms with treasury products are top-tier Auto Pro targets. Score `company_fit` at 60.

* **Hard Constraint – Mega-Cap / Diversified Tech (buyer_fit_negative):** Mega-cap tech (Meta, Google, Amazon) AND massive payment gateways (PayPal, Stripe) without a specific digital-assets / treasury / fintech mandate are too broad to be direct buyers. Cap `company_fit` at 24.

* **Hard Constraint – Service Providers vs. BaaS/Payments Consultants (company_mismatch rescue):** Generic consultancies, law firms, recruiting agencies, advisory shops, accounting firms, marketing agencies: cap `company_fit` at 12. EXCEPTION: "BaaS consultants", "payments consultants", and "deep fintech" advisory firms are highly relevant. Score `company_fit` 36–48 for specialized BaaS/payments/fintech consultants.

* **Access is company-side here.** Alumni signal, warmth, relevant connector value, mutual count, and degree contribute to company_fit via the thresholds below. Followers and general visibility do not count. `Cal+CBS` > `CBS` > `Cal` > blank. Without direct evidence of prior interaction or a strong mutual network, warmth should not push company_fit into the top two bands on reachability alone.

### Role-side constraints (all affect role_fit)

* **Hard Constraint – Investor-Only & CVC (allocator_mismatch):** Pure VC partners, angel investors, Corporate Venture Capital (CVC) / Venture Banking heads (e.g., at Banco BS2), and fund managers whose mandate does not include treasury / allocation decisions: cap `role_fit` at 16. General seniority and "investor" branding do not equal buying authority.

* **Hard Constraint – Hybrid Operator/Investor/Advisor (allocator_mismatch rescue):** Hybrid "operator, investor, advisor" profiles (e.g., VP Executivo at Organizze) who actively advise or operate in fintech/payments should NOT be penalized as investor-only. Score `role_fit` 32-40 for these hybrids.

* **Hard Constraint – Sabbatical / Past Role Rescue:** For candidates on sabbatical, score `role_fit` based on their most recent operating role (e.g., Founder, Creator of Open Banking). Score `role_fit` 32-40.

* **Hard Constraint – Operator Allocator Floor:** An operator at a relevant fintech (BaaS, payments, digital banking, custody, exchange, institutional crypto) who owns product, partnerships, treasury, or distribution decisions gets `role_fit` minimum 32. Do not dock role_fit just because the person is not an "investor" — operating authority over adoption decisions is the strongest allocator signal.

* **Service Provider Roles:** Generic consultants, recruiters, law-firm advisors, accountants, marketers: cap `role_fit` at 8. Specialized BaaS/payments consultants can score up to 32.

* **Mega-Cap Tech / Massive Payments Roles:** VP / Director / exec at Meta, Google, Amazon, PayPal, Stripe without an explicit digital-assets / treasury charter: cap `role_fit` at 16 regardless of title.

* **Functional Alignment:** VP / Director of partnerships, product, treasury, digital assets, or strategy at a relevant fintech = top-band role_fit. Role-to-Auto-Pro alignment beats title seniority.

## Direct Point Maps

The two FT dimensions sum to exactly 100. family_office_relevance is NOT part of this rubric.

FT score point maps (sum of caps = 100):

* company_fit = 12, 24, 36, 48, 60
* role_fit = 8, 16, 24, 32, 40

## Score Bands

* qualified = 75-100
* nearly_qualified = 50-74
* little_qualified = 25-49
* totally_unqualified = 0-24

## Dimension Guidance

### company_fit

* 60: Current company is a BaaS platform, payment processor, digital banking platform, custody / exchange with treasury products, institutional digital-finance infrastructure company, or B2B payment network. Assign 60 for direct-fit companies. Also: warm reachable account pushes a 48-tier company into 60.
* 48: Strongly relevant one step from top tier: institutional crypto with treasury / payments product (Ripple, Circle, Paxos), emerging-market fintech (Nubank, dLocal, Mercado Livre credit), "credit as a service" platforms, major payment networks (Visa, Mastercard, Worldpay), embedded fintech platforms. Also: candidates on sabbatical whose immediate past company was a top-tier fintech (e.g., PicPay, Guiabolso).
* 36: Directionally relevant but indirect: crypto infrastructure / security (Fireblocks), mixed-fit fintech, traditional bank with a specific digital / fintech mandate, or deep fintech advisory firms.
* 24: Weak but visible: retail crypto exchanges (Mercado Bitcoin, Foxbit), core banking software (Pismo), credit scoring platforms (Base39), lending infrastructure (8fig), FX/remittance firms (Ebury Bank), mega-cap tech (Meta / Google / Amazon), massive payment gateways (PayPal, Stripe) without embedded fintech focus, pure crypto / DeFi without institutional products, generic big bank without a digital mandate.
* 12: Service provider, generic consultancy, law firm, recruiting agency, AI company, or no longer in fintech.

Rules:

* Use the current-company block first. If the current company is missing, vague, or generic, do not inflate the score based on the person alone.
* If the current company looks like a generic consultancy, recruiter, advisory shop, law firm, AI company, or credit scoring platform, keep company_fit at 12 unless hard evidence shows a real Auto Pro buying or distribution mandate.
* Pure crypto / DeFi without institutional products is 24, not 36. Crypto companies WITH institutional payments / stablecoin / treasury products are 48, not 24.
* Emerging-market fintechs (LatAm, SEA, Africa, India) with credit, payments, or treasury products, as well as "credit as a service" platforms, are 36–48, not 24.
* Channel vs buyer: BaaS / payments channel profiles are buyers at 48–60. Generic crypto infra / security channels stay at 36. Core banking software and credit infra cap at 24.
* Access is folded in as a modifier at the top two bands: without warmth evidence, do not push a 36-tier company above 48 based on reachability alone.

### role_fit

* 40: Current role is directly aligned with treasury, product, digital-assets infrastructure, partnerships, institutional distribution, strategy, or actual buying / adoption decision-making at a relevant company. VP / Director of partnerships or product at BaaS / payments = 40. Also: candidates on sabbatical whose immediate past role was a top-tier fintech founder/executive.
* 32: Strong role relevance with clear operating authority: operator / executive at BaaS / payments / digital banking who owns product, partnerships, or distribution decisions. Treasury / digital-assets role at a traditional bank with a real mandate. Also: hybrid "operator, investor, advisor" profiles with active fintech involvement.
* 24: Moderate role relevance, or strong role at a weaker / mismatched company. Partial influence, crypto infrastructure operator, bank executive on adjacent functions.
* 16: Weak role relevance or impressive-but-non-buyer title: VC partner / angel / board-only advisor without operating role, Corporate Venture Capital (CVC) / Venture Banking heads, VP at mega-cap tech / massive payments without digital-assets charter, crypto infra sales, investor-only profile.
* 8: Role is mostly irrelevant to the likely buyer or adoption motion. Generic service providers, recruiters, and pure advisors cap here.

Rules:

* Use current title and current position description first.
* Functional alignment beats title seniority. A VP / Director of partnerships, product, or treasury at a BaaS / payments company is 40, not 32 or 24.
* An impressive title at an irrelevant / mismatched company caps at 16.
* Mega-cap tech and massive payment gateway roles cap at 16 regardless of title.
* Investor-only / advisor-only profiles cap at 16. Operators at relevant fintechs floor at 32. Hybrid "operator, investor, advisor" profiles score 32-40.
* Generic service providers and pure advisors cap at 8. Specialized BaaS/payments consultants can score up to 32.

## Calibration Notes

* Optimize for separating `Sent` from `Skip` on the two-axis FT score.
* `Cust` is not a valid outcome for this experiment; the binary is SKIP vs GOOD only.
* This rubric starts at caps **company_fit 60 / role_fit 40** based on the hypothesis that current-company-fit is the dominant signal and role is a multiplier. The autopilot may rebalance within the ±6 weight step per iteration so long as caps continue to sum to exactly 100.
* All six v7 systematic-error targets are preserved, just relocated:
  1. Mega-cap tech / massive payments false positive → caps on company_fit (24) AND role_fit (16).
  2. Pure crypto / DeFi / AI / credit scoring over-scoring → company_fit 12-24.
  3. Crypto-with-institutional-use-case rescue → company_fit 48–60.
  4. BaaS / payments / digital banking under-scoring → company_fit 60, operator role_fit floor 32.
  5. Investor-only and advisor over-scoring → role_fit ≤ 16; generic service provider role_fit ≤ 8. Hybrid operator/investor rescued to 32-40.
  6. Emerging-market fintech / credit-as-a-service under-scoring → company_fit 48–60.
* False negatives matter most when real fintech operators, BaaS / payments leaders, treasury owners, infrastructure leaders, or strong channel-adjacent leads fall below 75. The explicit rescues above directly target these.
* False positives matter most when generic senior profiles, service providers, advisors, pure crypto / DeFi, mega-cap tech, massive payment gateways, or investor-only profiles land above 75. The caps above directly target these.