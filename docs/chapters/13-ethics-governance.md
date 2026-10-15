# 13. Ethics, privacy & governance

!!! abstract "Learning objectives"
    - Identify ethical risks across the big data lifecycle: collection, linkage, modelling, deployment.
    - Explain why anonymisation is hard and how re-identification happens.
    - Apply k-anonymity and differential privacy to datasets and queries.
    - Summarise the key obligations of Rwanda's personal data protection law and related African frameworks.
    - Audit a model for performance gaps across groups.

## 13.1 Why big data raises new ethical problems

Big data changes the scale and nature of harm:

- **Linkage.** Individually harmless datasets (mobile phone location, mobile money, social media) become revealing when joined.
- **Inference.** Models infer sensitive attributes (health, ethnicity, income, sexual orientation) that were never collected.
- **Scale of error.** A biased rule applied to one million loan applicants harms at scale.
- **Power asymmetry.** Those who hold the data — governments, telecoms, platforms, often foreign — know far more than those described by it.
- **Representation.** Data from smartphones and the internet under-represents rural, older, poorer and female populations in many African countries; "big" data is not "complete" data.

## 13.2 The myth of anonymous data

Removing names is not anonymisation. Classic results:

- Sweeney (2000) estimated that 87% of the US population is uniquely identified by **ZIP code + gender + date of birth**.
- de Montjoye et al. (2013) showed that **four** spatio-temporal points are enough to uniquely identify 95% of individuals in a mobile phone dataset of 1.5 million people.
- de Montjoye et al. (2015) found similar results for credit card transactions.

Attributes that are not identifiers alone but identify in combination are **quasi-identifiers**.

```python
import pandas as pd
df = pd.read_parquet("survey.parquet")
qi = ["district", "sex", "age", "occupation"]
group_sizes = df.groupby(qi).size()
print("share of people unique on QIs:", (group_sizes == 1).sum() / len(df))
```

## 13.3 k-anonymity

A dataset is **k-anonymous** if every combination of quasi-identifier values appears at least \(k\) times. Achieve it by:

- **generalisation** — age 34 → 30–39; village → district;
- **suppression** — remove rare records or values.

```python
def k_anonymity(df, qi):
    return df.groupby(qi, observed=True).size().min()

df["age_band"] = pd.cut(df.age, bins=[0, 18, 30, 45, 60, 120], right=False)
print(k_anonymity(df, ["district", "sex", "age"]))        # often 1
print(k_anonymity(df, ["province", "sex", "age_band"]))   # much larger
```

Limitations: k-anonymity is vulnerable when all records in a group share the same sensitive value (homogeneity attack) — addressed by *l-diversity* and *t-closeness* — and it offers no guarantee against attackers with auxiliary data.

## 13.4 Differential privacy

Differential privacy (DP) is a mathematical guarantee about an **algorithm**, not a dataset. A randomised mechanism \(M\) is \(\varepsilon\)-differentially private if for all datasets \(D, D'\) differing in one person, and all outputs \(S\),

\[
\Pr[M(D) \in S] \le e^{\varepsilon}\,\Pr[M(D') \in S].
\]

Intuition: the output is almost equally likely whether or not *you* are in the data, so it reveals little about you. Smaller \(\varepsilon\) → more privacy, more noise.

### The Laplace mechanism

For a query \(f\) with **sensitivity** \(\Delta f = \max |f(D) - f(D')|\), release \(f(D) + \text{Laplace}(0, \Delta f/\varepsilon)\). A count has sensitivity 1.

```python
import numpy as np
rng = np.random.default_rng(0)

def dp_count(true_count, epsilon):
    return true_count + rng.laplace(0, 1 / epsilon)

def dp_mean(x, lower, upper, epsilon):
    x = np.clip(x, lower, upper)
    # split budget between sum and count
    s = x.sum() + rng.laplace(0, (upper - lower) / (epsilon / 2))
    n = len(x) + rng.laplace(0, 1 / (epsilon / 2))
    return s / max(n, 1)

true = 1234
for eps in (0.01, 0.1, 1, 10):
    print(eps, [round(dp_count(true, eps)) for _ in range(5)])
```

Key properties:

- **Composition.** Answering queries with budgets \(\varepsilon_1, \dots, \varepsilon_k\) costs \(\sum \varepsilon_i\) in total. Privacy is a budget you spend.
- **Post-processing.** Anything computed from a DP output remains DP.
- **Group size matters.** Noise of ±10 is negligible for a district count of 50,000 and destructive for a village of 30.

The US Census Bureau used DP for the 2020 census; tools include OpenDP and Google's DP libraries, and Tumult Analytics (which runs on Spark).

## 13.5 Fairness and bias

Bias enters at every stage: who is in the data (sampling), how labels were produced (historical discrimination), which features proxy for protected attributes (location often proxies for ethnicity or income), and how predictions are used.

### Group performance audit

```python
from sklearn.metrics import roc_auc_score, recall_score
def audit(df, group, y="label", p="score", thr=0.5):
    rows = []
    for g, d in df.groupby(group):
        rows.append({group: g, "n": len(d), "base_rate": d[y].mean(),
                     "auc": roc_auc_score(d[y], d[p]) if d[y].nunique() > 1 else np.nan,
                     "tpr": recall_score(d[y], d[p] >= thr),
                     "flag_rate": (d[p] >= thr).mean()})
    return pd.DataFrame(rows)

print(audit(scored_pdf, "urban_rural"))
```

Common fairness criteria — demographic parity (equal flag rates), equal opportunity (equal TPR), calibration within groups — **cannot all hold at once** when base rates differ (Kleinberg et al. 2016; Chouldechova 2017). Choosing among them is a value judgement that must involve affected communities, not only data scientists.

## 13.6 The legal landscape

!!! warning "Not legal advice"
    This section is an educational summary. Laws and regulations change; for real projects, consult the official texts, the supervisory authority's guidance, and your institution's legal or ethics office.

### Rwanda: Law No. 058/2021 on the protection of personal data and privacy

Rwanda's law, published in October 2021 and fully applicable after a two-year transition, follows principles similar to the EU's GDPR. Key points for data scientists:

- **Supervisory authority:** the National Cyber Security Authority (NCSA). Data controllers and processors must **register** with it.
- **Principles:** lawfulness, fairness and transparency; purpose limitation; data minimisation; accuracy; storage limitation; integrity and confidentiality; accountability.
- **Lawful bases** for processing include consent, contract, legal obligation, vital interests, public interest and legitimate interests.
- **Sensitive personal data** (for example health, genetic and biometric data, ethnic or social origin, religious or political beliefs, sexual life, criminal records) faces stricter conditions.
- **Data subject rights** include access, rectification, erasure, objection, data portability, and not being subject to decisions based solely on automated processing with significant effects.
- **Data breaches** must be notified to the authority promptly (the law sets a short deadline, measured in hours), and to affected persons where there is high risk.
- **Storage and transfer outside Rwanda** are restricted: storing personal data outside Rwanda requires authorisation from the NCSA, and cross-border transfers need an adequate legal basis or safeguards.
- **Data protection impact assessments** are expected for high-risk processing — large-scale profiling and big data projects often qualify.

**Implications for your projects:** before using phone, health, financial or survey microdata, establish the legal basis, minimise and pseudonymise, check where cloud storage physically resides, document a DPIA, and obtain ethics approval (e.g., from your institution's research ethics committee, and the Rwanda National Ethics Committee for health research).

### Wider frameworks

- **African Union Convention on Cyber Security and Personal Data Protection** (Malabo Convention, adopted 2014, in force since 2023).
- **AU Data Policy Framework** (2022) — promotes data flows within Africa alongside protection and sovereignty.
- National laws across the continent, e.g., Kenya's Data Protection Act (2019), Nigeria's Data Protection Act (2023), South Africa's POPIA.
- **EU GDPR** applies when processing data of people in the EU — relevant for international collaborations.
- Rwanda's national policies on data (e.g., the Data Revolution Policy) and on artificial intelligence (National AI Policy) set the government's direction on open data, sharing and responsible AI.

## 13.7 Governance in practice

| Practice | What it means |
|---|---|
| Data inventory and classification | Know what you hold; label public / internal / personal / sensitive |
| Access control | Least privilege; role-based access; audit logs |
| Pseudonymisation | Replace identifiers with keyed hashes (HMAC with a secret key — plain hashes of phone numbers are reversible by brute force) |
| Retention | Delete data when its purpose ends; lakehouse `DELETE` + `VACUUM` makes this feasible |
| Lineage and documentation | Datasheets for datasets, model cards for models |
| Open data with care | Publish aggregates with suppression or DP, not microdata |

```python
import hmac, hashlib, os
SECRET = os.environ["PSEUDO_KEY"].encode()      # store in a secrets manager, never in code
def pseudonymise(msisdn: str) -> str:
    return hmac.new(SECRET, msisdn.encode(), hashlib.sha256).hexdigest()[:16]
```

## 13.8 Case studies for discussion

1. **Mobile phone data for poverty mapping.** Studies (e.g., Blumenstock, Cadamuro & On, *Science*, 2015 — using Rwandan phone data) predicted wealth from call records. Discuss: consent, benefit to subscribers, risk of misuse, accuracy for people without phones.
2. **Mobility data during COVID-19.** Aggregated mobility statistics guided lockdown policy in several African countries. What aggregation and governance made this acceptable? What could go wrong?
3. **Credit scoring from smartphone data.** Digital lenders in East Africa score borrowers from phone metadata. Discuss transparency, the right not to be subject to automated decisions, and debt-collection practices.
4. **Satellite imagery and privacy.** Building-level mapping supports planning and disaster response, but can also enable surveillance or eviction. Where is the line?

## Lab 13 — Privacy in practice

1. Take a microdata extract (a public-use survey file from NISR or the DHS program, or a synthetic dataset generated with `faker`). Compute the share of unique records on a set of quasi-identifiers.
2. Generalise and suppress to reach k = 10. Report the information lost (e.g., change in a regression coefficient or a cross-tab).
3. Release counts per district with the Laplace mechanism for \(\varepsilon \in \{0.1, 1, 10\}\). Plot relative error versus true count.
4. Audit a model from Chapter 8 for performance differences across a grouping variable (borough, or urban/rural in your own data).
5. Write a one-page DPIA-style note for your capstone project: data, purpose, legal basis, risks, mitigations.

## Check yourself

1. Why are hashed phone numbers not anonymous?
2. A query engine allows unlimited DP queries at \(\varepsilon = 0.1\) each. What is the problem?
3. Under Rwanda's law, what should you check before storing personal data in a cloud region in Europe?

## Further reading

- Dwork & Roth (2014). *The Algorithmic Foundations of Differential Privacy* (free online).
- Barocas, Hardt & Narayanan. *Fairness and Machine Learning* (free at fairmlbook.org).
- de Montjoye et al. (2013). *Unique in the Crowd: The privacy bounds of human mobility*. Scientific Reports.
- Gebru et al. (2021). *Datasheets for Datasets*. Communications of the ACM.
- Official text of Law No. 058/2021 (Official Gazette of Rwanda) and guidance from the NCSA: [cyber.gov.rw](https://cyber.gov.rw/)
