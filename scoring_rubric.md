# Lead Scoring Rubric v3

## Output

Return four raw scores only:

1. `fo_persona`
2. `ft_persona`
3. `allocator`
4. `access`

All scores must be integers from 0 to 5.
Do not calculate composite formulas.

## Core reading rules

* Read the entire record.

* Use current role, current company, headline, summary, historical positions, historical organizations, organization titles, and organization descriptions together.

* Count synonyms, equivalent phrases, translated terms, and near-synonyms.

* Do not require exact literal wording if the meaning is clear.

* A person can score high on Persona and low on Allocator.

* A person can score high on Access and low on Persona.

* A trusted service provider to family offices or UHNW clients can score high on Family Office Persona even if they are not the direct capital allocator.

* Use `Mutual Count` and `Degree` as evidence for Access, but do not collapse them into the score itself.

* `Alumni Signal` values are only `Cal+CBS`, `Cal`, `CBS`, or `NULL`.

## Synonyms and equivalent language count

Treat meaningfully similar terms as evidence.
Examples include, but are not limited to:

* Family Office equivalents: private office, private capital office, single family office, multi family office, family capital platform, family investment office, family enterprise office

* Wealth / UHNW / advisory equivalents: private wealth, wealth management, UHNW advisory, patrimonial, estate planning, succession planning, fiduciary, trustee, trust and estates, private client, legacy planning, governance for family capital

* Investment / allocator equivalents: capital, investments, investing, investor, portfolio, fund, treasury, allocation, investment committee, CIO office, deputy CIO, principal, partner, managing director, portfolio manager

* Fintech equivalents: payments, embedded finance, digital banking, banking infrastructure, treasury infrastructure, wallet, neobank, stablecoin, blockchain infrastructure, financial rails, digital assets infrastructure

## PROPERTY 1: Persona Signal - Family Office

This workflow intentionally treats the Family Office lane broadly.
A clear investment-firm, wealth-firm, or family-capital-services lane is in scope.

### 5

Explicit Family Office or clear FO-equivalent role **plus** clear senior investing, allocation, CIO-office, portfolio, treasury, or capital oversight authority.

Typical examples:

* Founder / Partner / Principal / CIO / Deputy CIO / Managing Director / Head of Investments at a Family Office

* Investor / Investing / Portfolio Manager / Investment Committee role at a verified Family Office or FO-equivalent platform

* Senior trusted advisor whose daily work directly shapes family capital decisions at the highest level

### 4

Strong Family Office or family-capital relevance, but not the clearest top-band senior allocator case.

Typical examples:

* Explicit Family Office role with meaningful investment or advisory relevance

* Wealth / UHNW / allocator platform with strong family-capital relevance

* Trust and estates lawyers, private client lawyers, estate planners, trustees, tax / structuring counsel, or family governance advisors who work directly with UHNW or Family Office decision-makers

* Service providers whose work is embedded in the Family Office buying and decision process

### 3

Minimum score for any credible investment-firm, wealth-firm, capital-firm, allocator, or serious family-capital-adjacent lane.

This includes roles and firms with terms such as:

* capital

* investments

* investing

* investor

* wealth

* asset management

* gestora

* portfolio

* private bank

* RIA

* OCIO

* investment committee

* CIO / Deputy CIO

A 3 means the person is clearly in-scope for this workflow, even if the company is not literally called a Family Office.

### 2

Weak but still visible relevance.
The person sits near the lane but does not clearly operate in it.

Examples:

* light financial adjacency

* vague advisory relevance

* support exposure to investment or UHNW contexts without strong evidence of buyer relevance

### 1

Very remote relevance.
Only faint or indirect signs of Family Office, wealth, allocator, or family-capital relevance.

### 0

No credible Family Office, wealth, capital, allocator, or family-capital-services relevance at all.

### Family Office rules

* Any credible investment-firm / wealth-firm / capital-firm lane should usually be at least **3**.

* Explicit Family Office language should usually push the score to **4 or 5**.

* Service providers can score **4** when they are deeply relevant to Family Office decisions, even if they are not direct allocators.

* Do not confuse direct allocator power with Persona. Persona measures lane relevance, not only buying authority.

## PROPERTY 2: Persona Signal - Fintech

This score measures how directly the person sits in a fintech, payments, banking infrastructure, treasury infrastructure, digital finance, or institutional transformation lane.

### 5

Mainstream financial institution or scaled platform with explicit current role in digital finance, payments, banking infrastructure, treasury infrastructure, embedded finance, product, partnerships, distribution, or institutional transformation that is highly relevant to adoption.

### 4

Explicit fintech / payments / neobank / wallet / infrastructure / crypto / blockchain / stablecoin role or company with strong relevance.
High-quality evaluator, operator, influencer, or channel partner.

### 3

Clear fintech ecosystem relevance.
This includes:

* traditional bank roles in product, strategy, partnerships, operations, treasury, digital, innovation, or commercial lanes

* fintech vendors / infrastructure / data / advisory / VC / CVC / ecosystem nodes with clear fintech relevance

* crypto companies or crypto roles that are relevant, but not the ideal end-buyer

### 2

Weak but visible fintech adjacency.
There is some plausible relationship to fintech, payments, digital finance, or institutional financial transformation, but it is not strong.

### 1

Very remote fintech relevance.

### 0

No credible fintech relevance.

### Fintech rules

* Traditional banks are in scope.

* Ecosystem roles are in scope.

* Explicit crypto roles count, but crypto-native does not automatically mean a 5.

* 5 should usually be reserved for highly relevant buyers, transformation leaders, or very strong fintech platforms.

## PROPERTY 3: Allocator Score

This measures decision power, approval power, budget power, sponsorship power, and go / no-go influence.

### 5

Direct owner of capital allocation, product go/no-go, treasury deployment, major partnership approval, or clearly relevant strategy / budget decisions.

### 4

Strong decision-maker or clear budget owner.
Can sponsor or materially advance adoption.

### 3

Evaluator, recommender, diligence participant, or meaningful internal influencer.
Can affect the outcome, but is not the primary owner.

### 2

Adjacent operator, gatekeeper, manager, analyst, or specialist with partial influence.

### 1

Minimal influence.

### 0

No meaningful evidence of decision influence.

### Allocator rules

* Seniority alone is not enough.

* Prestige alone is not enough.

* Founder / CEO / President can still be 2 or 3 if the mandate is unclear.

* Company type alone does not raise Allocator.

* The key question is whether this person can approve, block, sponsor, or materially shape the relevant buying or adoption decision.

## PROPERTY 4: Access Score

This measures how reachable the person is through alumni, warmth, mutuals, degree, connector value, and demonstrated reachability.

### 5

Active warm relationship, prior real interaction, clearly live warm path, or unusually strong relevant connector path already in motion.

### 4

Direct connection plus strong warm path, or very strong relevant reachability through alumni plus credible connector path.

### 3

Credible moderate reachability.
This is a common outcome for meaningful alumni or decent connector value.

### 2

Weak reachability.
Some mutuals, some relevant network value, or a limited path.

### 1

Cold or difficult reachability.
Very little demonstrated relevant access.

### 0

Essentially unreachable from available evidence.

### Access rules

* Alumni matters a lot.

* `Cal+CBS` is stronger than `CBS`, which is stronger than `Cal`, which is stronger than `NULL`.

* Mutual Count is a useful proxy, but should not overwhelm stronger direct signals.

* Relevant connector value beats vanity metrics.

* Followers and generic visibility do not count.

* Advisors and service providers only improve Access when they truly improve reachability.

* Degree is only one weak-to-moderate input to Access Score.

* It is not the score itself.

* `1 = first-degree path exists`

* `2 = second-degree path exists`

* `3 = no path found in the sidecar / unknown / not found`

* A `3` does not mean low access by itself.

* A person can still have Access `3-5` based on alumni overlap, warm connector value, prior interaction, strong mutual cluster, or other evidence.

* A `1` does not automatically mean Access `4` or `5` either.

* Remove any formula thinking. Access Score is a holistic judgment using:

* alumni signal

* warm path / prior interaction

* mutual count

* Degree

* connector relevance

* demonstrated reachability

* `Degree = 1` can support a higher Access score.

* `Degree = 2` is neutral-to-positive.

* `Degree = 3` is only a mild negative unless other reachability evidence is also weak.

* Do not let degree define the band.

## Calibration examples

* Partner at a Family Office with investment committee responsibility, CBS alumni, good mutuals -> high FO Persona, high Allocator, at least moderate Access.

* Private client / trust and estates lawyer serving UHNW families -> high FO Persona, lower Allocator than a CIO, Access depends on the full reachability picture, not Degree alone.

* CIO at a generic asset manager -> at least 3 on FO Persona for this workflow, strong Allocator, but not automatically a 4 or 5 on FO Persona unless family-capital relevance is clear.

* Head of digital at a major bank -> high Fintech Persona, likely meaningful Allocator, Access depends on alumni, warmth, connector value, mutuals, and Degree together.

* Random operator at a non-financial company -> likely 0 on FO Persona and 0 on Fintech Persona unless the history clearly says otherwise.