# Lead Scoring Rubric v7

## Product Definition

Auto Pro is Auto.finance's institutional and white-label offering: the same core
autopool infrastructure, packaged for institutions such as family offices,
fintechs, custodians, exchanges, funds, and other professional investors.

The clearest anchors today are:

* `autoUSD`: Treasury-plus stablecoin yield

* `autoETH`: staking-plus ETH yield

The current evaluation objective is fintech-first. Family office relevance still
matters, but the calibration target is to separate true Auto Pro fintech buyers,
operators, and high-value discovery conversations from generic finance,
crypto-prestige, advisory, or investor-branded profiles.

## Output

Return one JSON object with a top-level `results` array.

Each result must include exactly these keys:

* `urn`

* `company_fit`

* `family_office_relevance`

* `fintech_relevance`

* `allocator_power`

* `access`

* `role_fit`

Rules:

* `urn` must exactly equal the input `urn` string.

* Use only the allowed point values from `Direct Point Maps`.

* Return no prose, no markdown, and no derived explanations.

## Core Rules

* Score against Auto Pro specifically, not generic crypto prestige, generic fintech prestige, or generic executive impressiveness.

* Current company and current role are the primary decision surface. Current company description and current position description matter more than older biography.

* The additional organization block is supporting evidence, not the main anchor, unless the current-company block is genuinely thin.

* **Hard Constraint – Mega-Cap / Diversified Tech (buyer\_fit\_negative):** Mega-cap tech (e.g., Meta, Google, Amazon) or massive diversified banks without a specific digital-assets/treasury/fintech mandate are too broad to be direct buyers. Cap `company_fit` at 12, `fintech_relevance` at 10, `allocator_power` at 6, and `role_fit` at 2. These profiles must not reach qualified.

* **Hard Constraint – Pure Crypto / DeFi (company\_mismatch):** Pure-play crypto exchanges, DeFi protocols, or crypto trading firms without an institutional yield/treasury/stablecoin/payments product are not Auto Pro buyers. Cap `company_fit` at 12 and `fintech_relevance` at 10. This is stricter than v6: pure DeFi trading is a mismatch, not just "directionally relevant."

* **Hard Constraint – Crypto with Institutional Use Case (company\_mismatch rescue):** Crypto companies whose core product is institutional payments, remittance, stablecoin issuance, or treasury infrastructure (e.g., Ripple, Circle, Paxos) are NOT pure DeFi mismatches. Score `company_fit` 18–24 and `fintech_relevance` 15–20. The presence of an institutional product line overrides the "crypto" label.

* **Hard Constraint – Crypto Infrastructure / Channel (channel\_vs\_buyer):** Crypto infrastructure/security providers (e.g., Fireblocks) are adjacent channels but not treasury deployers. Cap `company_fit` at 18 and `allocator_power` at 6. HOWEVER: BaaS platforms, payments processors, and digital-banking infrastructure labeled "channel" are high-value targets, not second-class leads. For BaaS/payments channel profiles, score `company_fit` 24–30, `fintech_relevance` 20–25, and `allocator_power` 9–12. Do not penalize a profile just because it is a channel — if it controls float, treasury, or distribution, it is a buyer.

* **Hard Constraint – BaaS / Payments / Digital Banking (customize\_not\_skip):** Banking-as-a-Service platforms, payment processors, digital banking platforms, B2B payment networks, and custody/exchange platforms with treasury products are top-tier Auto Pro targets. Score `company_fit` at 30, `fintech_relevance` at 25. Operators at these companies get `allocator_power` minimum 9 and `role_fit` minimum 4. These profiles must not score below nearly\_qualified.

* **Hard Constraint – Service Providers (service\_provider):** Consultancies, law firms, recruiting agencies, advisory shops, accounting firms, and marketing agencies are not buyers. Cap `company_fit` at 6, `allocator_power` at 3, and `role_fit` at 1. Exception: a service provider whose practice is exclusively digital-assets/fintech strategy and who directly influences buying decisions may reach `company_fit` 12 and `allocator_power` 6, but no higher.

* **Hard Constraint – Investor-Only Profiles (allocator\_mismatch):** VC partners, angel investors, board advisors without an operating role, and fund managers whose mandate does not include treasury/allocation decisions get `allocator_power` max 6. General seniority and "investor" branding do not equal buying authority. A Partner at a VC firm is a 6 on allocator\_power unless evidence shows direct allocation/treasury control.

* **Hard Constraint – Operator Allocator Floor:** An "operator" at a relevant fintech (BaaS, payments, digital banking, custody, exchange, institutional crypto) who owns product, partnerships, treasury, or distribution decisions gets `allocator_power` minimum 12. Do not dock allocator\_power just because the person is not an "investor" — operating authority over adoption decisions is the strongest allocator signal.

* **Emerging-Market Fintechs:** LatAm, SEA, African, and Indian digital banks, payments platforms, and credit/fintech arms of large tech companies (e.g., Mercado Libre's credit division, Nubank, dLocal) are in scope. Score `company_fit` 18–24 and `fintech_relevance` 15–20 when the role involves treasury, payments, credit, or digital-assets infrastructure.

* Access should use alumni signal, warmth, relevant connector value, mutual count, and degree together. Followers and general public visibility do not increase Access. Be conservative: without direct evidence of prior interaction or a strong mutual network, access should not exceed 6.

## Direct Point Maps

* <span data-proof="suggestion" data-id="m1776714917198_5" data-by="ai:external-agent" data-kind="replace">company_fit = 6, 12, 18, 24, 30</span>

* <span data-proof="suggestion" data-id="m1776714917198_5" data-by="ai:external-agent" data-kind="replace">family_office_relevance = 3, 6, 9, 12, 15</span>

* <span data-proof="suggestion" data-id="m1776714917198_5" data-by="ai:external-agent" data-kind="replace">fintech_relevance = 5, 10, 15, 20, 25</span>

* <span data-proof="suggestion" data-id="m1776714917198_5" data-by="ai:external-agent" data-kind="replace">allocator_power = 3, 6, 9, 12, 15</span>

* <span data-proof="suggestion" data-id="m1776714917198_5" data-by="ai:external-agent" data-kind="replace">access = 2, 4, 6, 8, 10</span>

* role\_fit = 1, 2, 3, 4, 5<span data-proof="suggestion" data-id="m1776706519711_1" data-by="ai:external-agent" data-kind="insert">
  FO score point map (independent; does not affect FT score):
  family_office_relevance = 3, 6, 9, 12, 15</span>

## Score Bands

* qualified = 75-100

* nearly\_qualified = 50-74

* little\_qualified = 25-49

* totally\_unqualified = 0-24

## Dimension Guidance

### company\_fit

* 30: Current company is a BaaS platform, payments processor, digital banking platform, custody/exchange with treasury products, or institutional digital-finance infrastructure company. This is the top tier — assign 30, not 24, for direct-fit companies.

* 24: Current company is strongly relevant but one step from top tier: B2B payment network, institutional digital-assets platform, crypto-with-institutional-product (e.g., Ripple, Circle), or emerging-market fintech with treasury/payments relevance.

* 18: Current company is directionally relevant but indirect: crypto infrastructure/security (e.g., Fireblocks), mixed-fit fintech, traditional bank with a specific digital/fintech mandate, or fintech-adjacent platform.

* 12: Current company is weakly relevant, too broad (mega-cap tech like Meta/Google), pure crypto/DeFi without institutional products, generic big bank without a digital mandate, or generic investment firm without a fintech/digital-assets mandate.

* 6: Current company is a service provider, consultancy, law firm, recruiting agency, advisory shop, or has no credible relevance to Auto Pro.

Rules:

* Company-level fit is the gatekeeper dimension. A weak company caps the entire profile regardless of biography.

* Use the current-company block first. If the current company is missing, vague, or generic, do not inflate the score based on the person alone.

* If the current company looks like a consultancy, recruiter, advisory shop, law firm, services vendor, or generic investment firm, keep company\_fit at 6 unless hard evidence shows a real Auto Pro buying or distribution mandate.

* Pure crypto/DeFi without institutional products is a 12, not 18. The v6 cap of 18 was too generous and created false positives.

* Crypto companies with institutional payments, stablecoin, or treasury products are a 24, not a mismatch. Differentiate by product, not by "crypto" label.

* Emerging-market fintechs (LatAm, SEA, Africa, India) with credit, payments, or treasury products are 18–24, not 12.

### family\_office\_relevance

* 15: Explicit family office or deeply family-capital-centric current lane with strong allocator relevance.

* 12: Strong UHNW, private wealth, private capital, trust, estate, or family-capital-services relevance.

* 9: Real but moderate family-office or private-capital adjacency.

* 6: Weak adjacency (includes VCs, private equity, and generic investors without explicit family capital evidence).

* 3: No credible family-office relevance.

Rules:

* This dimension is secondary to fintech relevance in the current eval loop. It barely separates Sent from Skip and should not be the reason a profile reaches qualified.

* VCs, private equity, and generic investors are NOT family offices; cap at 6 unless explicit family-capital evidence exists.

* Do not let a high family\_office\_relevance score compensate for low company\_fit or low fintech\_relevance.

### fintech\_relevance

* 25: Explicit BaaS, payments, digital banking, banking infrastructure, treasury infrastructure, custody, exchange, wallet, stablecoin, embedded-finance, or institutional digital-assets role/company with direct buyer or operator relevance. Assign 25, not 20, for direct-fit profiles.

* 20: Strong fintech relevance one step from ideal: B2B payments, institutional crypto with treasury products, crypto-with-institutional-use-case (e.g., Ripple), or emerging-market fintech with payments/credit relevance.

* 15: Real fintech ecosystem relevance: crypto infrastructure/security, strong adjacency in banks/infrastructure vendors, or traditional bank with digital mandate.

* 10: Weak but visible fintech adjacency, pure DeFi, pure crypto trading without institutional products, or mega-cap tech with crypto side-projects.

* 5: No credible fintech relevance.

Rules:

* This is the primary persona dimension for the current calibration set.

* Traditional banks are in scope when the role is clearly tied to digital products, payments, treasury, innovation, infrastructure, partnerships, or strategic transformation.

* Pure crypto/DeFi trading maxes out at 10. This is stricter than v6 (which allowed 15) because pure DeFi profiles were generating false positives.

* Crypto companies with institutional products (payments, stablecoin, treasury) are 15–20, not 10. Differentiate by product line.

* Emerging-market fintechs with credit, payments, or treasury products are 15–20.

* Mega-cap tech (Meta, Google, Amazon) maxes out at 10 regardless of crypto side-projects.

### allocator\_power

* 15: Direct owner of allocation, treasury deployment, float management, budget, partnership approval, product go/no-go, or institutional sponsorship.

* 12: Strong internal sponsor, executive owner, or operator at a relevant fintech who can materially advance a decision. This is the floor for operators at BaaS/payments/digital-banking companies who own product, partnerships, or treasury decisions.

* 9: Evaluator, recommender, diligence participant, or meaningful influencer. Also the floor for operators at BaaS/payments companies in relevant roles.

* 6: Partial influence only, crypto infrastructure provider, advisor, or investor-only profile. VC partners, angel investors, and board advisors without operating authority cap here.

* 3: Minimal or no meaningful decision influence. Service providers, consultants, recruiters, and pure advisors cap here.

Rules:

* Seniority alone is not enough. Prestige alone is not enough. "Partner at VC" is a 6 unless evidence shows direct allocation/treasury control.

* Founder, CEO, or Partner can still be 6 if the current mandate is not clearly relevant to adoption or buying.

* Do not confuse investor branding or general seniority with direct buying authority.

* An operator at a relevant fintech who controls product, partnerships, or distribution decisions is a 12–15, even if their title is VP or Director, not C-suite.

* Service providers and pure advisors are a 3. No exceptions without hard evidence of buying authority.

### access

* 10: Very strong warm path: prior interaction, direct relationship, or unusually strong reachable connector path with evidence.

* 8: Strong reachable path with clear signal (shared company, strong mutual network, warm introduction likely).

* 6: Moderate reachable path. This is the default for profiles with some mutual connections but no direct warmth signal.

* 4: Weak path. Generic LinkedIn connectivity, few mutuals, no warmth signal.

* 2: Mostly cold path.

Rules:

* `Cal+CBS` is stronger than `CBS`, which is stronger than `Cal`, which is stronger than blank.

* Mutual Count helps, but should not overpower clearer relationship signals.

* Degree is only one input. Followers and generic visibility do not count.

* Be conservative: access is slightly higher for Skip profiles than Sent profiles in calibration data. Do not let access inflate scores for otherwise weak profiles. Without direct evidence of warmth, cap at 6.

### role\_fit

* 5: Current role is directly aligned with treasury, product, digital-assets infrastructure, partnerships, institutional distribution, strategy, or actual decision-making relevant to Auto Pro. VP/Director of partnerships or product at a BaaS company is a 5 — seniority level does not matter for this rating.

* 4: Strong role relevance: operator/executive at a BaaS/payments/digital-banking company, or treasury/digital-assets role at a traditional bank. This is the floor for relevant roles at top-tier fintech companies.

* 3: Moderate role relevance, or strong role at a weak/mismatched company.

* 2: Weak role relevance, or impressive-but-non-buyer title (e.g., VP at Meta, Partner at VC, crypto infra sales, investor at family office without allocation authority).

* 1: Role is mostly irrelevant to the likely buyer or adoption motion. Service providers, recruiters, consultants, and advisors cap here.

Rules:

* Use the current title and current position description first.

* A great company with a peripheral role should not get 5.

* An impressive title at an irrelevant/mismatched company should not get above 2.

* A VP/Director of partnerships, product, or treasury at a BaaS/payments company is a 5, not a 3 or 4. The role-to-Auto-Pro alignment is what matters, not the seniority label.

* Mega-cap tech roles (Meta, Google) cap at 2 regardless of title.

## Calibration Notes

* Optimize for separating `Sent` from `Skip`.

* `Cust` is operationally useful but excluded from winner-selection metrics.

* **v7 changes target six systematic errors from the v6 dossier:**

  1. **Mega-cap tech false positive (buyer\_fit\_negative):** v6 capped only `company_fit` at 12. Meta still scored 78. v7 adds caps on `fintech_relevance` (10), `allocator_power` (6), and `role_fit` (2) for mega-cap tech. These profiles should now score well below 75.
  2. **Pure crypto/DeFi over-scoring (company\_mismatch):** v6 capped `company_fit` at 18 and `fintech_relevance` at 15. This was too generous. v6 pure DeFi is now capped at `company_fit` 12 and `fintech_relevance` 10. However, crypto-with-institutional-products (Ripple, Circle) is explicitly rescued at `company_fit` 18–24 and `fintech_relevance` 15–20.
  3. **BaaS/payments/digital banking under-scoring (customize\_not\_skip):** 12 of 15 customize\_not\_skip profiles were false negatives. v7 makes `company_fit` 30 (not 24–30 range) and `fintech_relevance` 25 mandatory for BaaS/payments/digital banking, with `allocator_power` floor of 9 and `role_fit` floor of 4 for operators. These profiles must not fall below nearly\_qualified.
  4. **Channel profiles under-scoring (channel\_vs\_buyer):** 6 channel\_vs\_buyer profiles were false negatives, including BaaS profiles labeled "better channel than buyer." v7 explicitly states that BaaS/payments channel profiles are high-value targets, not second-class leads. They get `company_fit` 24–30, `fintech_relevance` 20–25, `allocator_power` 9–12.
  5. **Investor-only and advisor over-scoring (allocator\_mismatch):** 10 allocator\_mismatch profiles were correctly Skipped, but the rubric wasn't aggressive enough. v7 caps `allocator_power` at 6 for investor-only profiles and 3 for service providers/advisors. General seniority and "Partner" titles do not override lack of operating authority.
  6. **Emerging-market fintech under-scoring:** LatAm fintech profiles (e.g., Mercado Libre credit) scored 32 despite being Sent. v7 explicitly includes emerging-market digital banks, payments, and credit platforms at `company_fit` 18–24 and `fintech_relevance` 15–20.

* **Access conservatism:** Access means are slightly higher for Skip than Sent in calibration data. v7 adds a default cap of 6 without direct warmth evidence. Do not let access inflate weak profiles into qualified.

* **family\_office\_relevance is low-signal:** Mean separation between Sent and Skip is only 0.53 points. This dimension should not be the reason a profile reaches qualified. High family\_office\_relevance does not compensate for low company\_fit or fintech\_relevance.

* False negatives matter most when real fintech operators, BaaS/payments leaders, treasury owners, infrastructure leaders, or strong channel-adjacent leads fall below 75. The explicit boosts for BaaS/payments, operator mandates, emerging-market fintechs, and crypto-with-institutional-products directly target these false negatives.

* False positives matter most when generic senior profiles, service providers, advisors, pure crypto/DeFi, mega-cap tech, or investor-only profiles land above 75. The expanded caps on mega-cap tech, stricter pure-DeFi caps, and service-provider hard constraints directly target these false positives.

<!-- PROOF
{
  "version": 2,
  "marks": {
    "m1776714917198_5": {
      "kind": "replace",
      "by": "ai:external-agent",
      "createdAt": "2026-04-20T19:55:17.199Z",
      "range": {
        "from": 5657,
        "to": 5838
      },
      "content": "The five FT dimensions have caps that sum to exactly 100. family_office_relevance is scored separately and is NOT part of the 100-point FT budget.\nFT score point maps (sum of caps = 100):\ncompany_fit = 7, 14, 21, 28, 35\nfintech_relevance = 6, 12, 18, 24, 30\nallocator_power = 4, 8, 12, 16, 18\naccess = 2, 5, 8, 10, 12",
      "status": "pending"
    },
    "m1776706519711_1": {
      "kind": "insert",
      "by": "ai:external-agent",
      "createdAt": "2026-04-20T17:35:19.727Z",
      "range": {
        "from": 5866,
        "to": 5968
      },
      "content": "\nFO score point map (independent; does not affect FT score):\nfamily_office_relevance = 3, 6, 9, 12, 15",
      "status": "pending"
    }
  }
}
-->

<!-- PROOF:END -->
