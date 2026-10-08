---
title: AirBreda — Architecture Design Document
---

# AirBreda — Architecture Design Document

*Correlating NO₂ at Luchtmeetnet station NL10240 (Breda-Tilburgseweg) with traffic at the A27 / Breda interchange. Cloud provider: AWS, region eu-north-1 (Stockholm). State of the system: 2 October 2026, as deployed.*

---

## 1. Architecture as deployed

![AirBreda architecture as deployed](images/airbreda-architecture.png)

**Data flow.** Every hour, cron on one EC2 VM starts two short-lived containers. `airbreda-air` (:10) fetches the latest NO₂ value, checks it for null or frozen values and writes it to PostgreSQL. `airbreda-traffic` (:20) downloads NDW's two feeds, extracts four A27 sites (`hrl`, `hrr` mainline; `vwd`, `vwa` ramps), stores one CSV per site plus the raw feeds in S3 and writes flow and speed rows to PostgreSQL. A long-running `dashboard` container serves `/site/{id}`, `/health` and an HTML page from the database, S3 and a model baked into its image. The model is trained on a laptop from the same database and bucket.

**Trust boundaries — who can access what**

| Principal | Can access | How it is authorised | Cannot |
|---|---|---|---|
| EC2 instance (role `airbreda-ec2-role`) | S3 objects in `airbreda-testnight-2026`: `s3:PutObject`, `s3:GetObject` | Custom IAM policy on `…:::airbreda-testnight-2026/*`; temporary credentials from instance metadata, no keys on the VM | List the bucket (verified: *AccessDenied*), any other bucket or service |
| Containers on the VM → database | PostgreSQL, port 5432 | RDS security-group rule whose source is the VM's security group; DB user and password in `.env` on the VM | — (see gaps) |
| Developer laptop (IAM user `airbreda-dev`) | Everything in the account; RDS port 5432; SSH to the VM | `AdministratorAccess` (verified 2 Oct); "My IP" security-group rules; SSH key pair | — |
| Any browser (incl. the grader) | Dashboard on port 8000 (read-only API and page) | VM security-group rule `0.0.0.0/0` on port 8000, plain HTTP, no authentication | SSH (port 22 limited to my IP), anything else on the VM |
| Luchtmeetnet, NDW | — | The VM only makes **outbound** requests to them | — |

**Known gaps** (mostly addressed in §6): the database is still *publicly accessible* (for laptop access), the containers use the database **master** user, the laptop user has administrator rights, and the dashboard is open to the internet over HTTP without authentication, a deliberate temporary exception for the assessment.

**Verified behaviour.** Cron runs unattended (11:10 and 11:20 UTC runs logged `db_write_success`). After narrowing the IAM role, the traffic job still uploaded 6 objects and the dashboard still read intensities from S3, while `aws s3 ls` was denied. The dashboard answers at `http://<vm-ip>:8000`. 34 automated tests pass. A reboot of the VM brought the dashboard back unattended (ADR-005).

---

## 2. Architecture Decision Records

### ADR-001 — Initial Data Storage Strategy (Day 1)

**Context.** Two sources: hourly NO₂ averages and NDW traffic (two gzipped XML feeds, ~1.5 MB and ~0.7 MB per download). Readings are structured, `(station_id, timestamp, component, value)`, and the core questions are time-range queries and **joins** of air quality with traffic. Volume is small: 11 hourly series ≈ 96,000 rows a year. A script may run twice for the same hour, and the parser will change, so history must be re-derivable.

**Decision.** Parsed readings go into **PostgreSQL on RDS** (`sensor_readings`, primary key `station_id, timestamp, component`). Raw files go into **S3**: one CSV per site per hour and the gzipped raw feeds. Duplicates are absorbed by **idempotent writes** (`INSERT … ON CONFLICT DO NOTHING`), not exactly-once delivery. All timestamps are UTC, and files are named by *measurement* time. Nulls are stored as NULL, never dropped or invented.

**Why.** A fixed schema, time-range queries and joins suit a relational database; NoSQL's write scaling solves a problem 96,000 rows a year does not have. Raw files are cheap, durable and an audit trail: a changed parser can re-process them, while the database holds only what the old parser extracted. Idempotent writes were proven when a laptop stack and the VM ran at once and duplicates were silently absorbed. Rejected: DynamoDB (no joins), database-only (raw data lost), files-only (no efficient queries).

**Consequences.** Simple, cheap, safe to re-run, history reprocessable. But `DO NOTHING` keeps the **first** value forever, so a source correction would be ignored. Two stores must stay consistent, and the 27-character NDW site IDs forced `station_id` to be widened from `VARCHAR(20)` to `VARCHAR(40)`.

### ADR-002 — Messaging Architecture (Day 2)

**Context.** More consumers of every reading are planned (database writer, ML, anomaly detection). Wiring each into the ingestion scripts means changing the producers every time, and a slow consumer could block ingestion. Volume: about 5 messages an hour (≈ 3,650 a month).

**Decision.** Ingestion **publishes each reading as JSON to a broker, alongside** its storage writes. For the Day 2 lab the broker is **Redis** (a list) in Docker Compose; for production the plan is **SNS → SQS**. Publishing is best effort: a failure is logged (`queue_publish_failed`) and the run continues.

**Why.** A broker decouples producers from consumers and isolates failures. Redis was chosen for speed of learning, but a Redis **list is a queue, not a topic**: two consumers would *split* the readings, not each receive all. It saves to disk only periodically, and the queue was lost when its container was removed. SNS → SQS gives fan-out, durability and dead-letter queues with no servers, at negligible cost.

**Broker down?** Storage writes still happen, so **no reading is lost from the database or bucket**. The queue only misses those messages, because there is no retry. A future fix is the *transactional outbox*.

**Flag versus drop.** Luchtmeetnet null or stale values are **kept and flagged** (`is_flagged`), because a visible flagged value beats a silent gap in a time series. NDW `speed = −1` sentinels are **dropped** (the raw feed stays in S3) so averages are not corrupted. Starting over, I would **keep and flag both**, storing the sentinel as NULL: dropping creates a gap that looks like "no data", and empty night-time ramps may legitimately report no valid speed.

**Consequences.** New consumers no longer touch producers; but two delivery paths can diverge and every consumer must be idempotent. *Partly superseded by ADR-004 on the VM.*

### ADR-003 — Resilience Strategy (Day 2)

**Context.** The December 2021 AWS us-east-1 event showed a whole region can be impaired for hours. AirBreda runs in one region with no cross-region copy. It is an informational dashboard: an outage harms nobody directly, but a long one makes it useless. Lost NO₂ hours can be backfilled from the Luchtmeetnet API. The NDW *live feed* holds only the current snapshot, so a missed run cannot be re-fetched from it; NDW's separate historical database (Dexter) might fill the gap, but I have not tested access.

**Decision.** **SLO for `/site/{id}`: 99.5 %** availability over 30 days (SLI: non-5xx answers within 2 s), an **error budget of 216 minutes a month**. **DR tier: Pilot Light**: data kept live in a recovery region (RDS cross-region read replica in eu-west-1, S3 replication), compute switched off; targets RTO ≤ 1 hour, RPO ≤ 5 minutes.

**Why.** 99.9 % (43 min a month) would need failover faster than the data even changes (hourly); 99 % (7.2 h a month) tolerates a whole working day of downtime. Backup & Restore cannot meet 99.5 %: one regional incident with a multi-hour restore uses the whole monthly budget. Warm Standby saves perhaps 30–45 minutes of recovery and costs **≈ $27 a month more** than Pilot Light (≈ $12–15), roughly 3× the DR spend for little user benefit. AWS describes Pilot Light as recovering in tens of minutes and Warm Standby in minutes.

**Consequences.** On paper it survives a regional failure within the SLO, but failover is a manual runbook that has **not been tested**, so the RTO is a hope. **This ADR is a target, not today's state**: there is no replica, and the single VM (ADR-004/005) cannot meet 99.5 % alone.

### ADR-004 — Compute Strategy (Day 3)

**Context.** The ingestion containers ran on a laptop, which sleeps and changes networks. They must run 24/7 next to the database and bucket. Workload: two jobs an hour, seconds to a minute each.

**Decision.** One **EC2 t3.micro** (Amazon Linux 2023) in **eu-north-1**, the same region as RDS and S3 (a second region would add latency, transfer charges and a dependency). **cron** starts each job as a fresh container (`docker run --rm`) that runs once and exits. S3 access via an **IAM instance role**, database access via a **security-group reference**. **No message broker.**

**Why a VM, and cost.** It shows the IaaS layer the course teaches, runs the existing images unchanged, and costs a predictable **≈ $12 a month**: instance $7.88, public IPv4 address $3.65 (easy to miss, ≈ 30 %), disk $0.64. **The queue was dropped** because there is no consumer, it is another process on a 1 GiB VM, and run-once containers have nothing to decouple. It returns with a second consumer, ingestion outgrowing one VM, or a need to buffer during database outages, as SNS → SQS.

**Unanticipated concern.** The traffic job was **killed silently for lack of memory** on the 1 GiB VM: its log stopped after one line, with no error, and `--rm` removed the container and its exit code. The NDW download took only 0.2 s, no container was left and there was no swap, so I added a 2 GiB swap file. Also: ~12 s of parsing per site (the whole XML is re-read for each site), no cron on Amazon Linux 2023, and Day 2's looping containers never exiting under cron (fixed: run-once by default).

**At 50 corridors, every 5 minutes.** First rewrite the parser (200 sites × 12 s ≈ 40 minutes per run cannot fit a 5-minute cycle), then move the jobs to **scheduled ECS Fargate tasks**, run the dashboard as two tasks behind a load balancer, and bring back **SNS → SQS**.

**Consequences.** Independent of the laptop, no AWS keys on the VM. But I patch an OS, logs live on one disk, nothing alerts on a missing run, and the VM is a single point of failure.

### ADR-005 — Compute & Deployment Strategy (Day 4) — *extends ADR-004*

**Context.** Day 4 adds a dashboard that must answer at any time, unlike the batch jobs. It needs the model, database and bucket, all reachable from the VM.

**Decision.** Run the dashboard as a **third container on the same VM** with `docker run -d --restart unless-stopped`; port 8000 is open to the internet for the assessment (SSH stays limited to one IP). Test the three containers locally with **Docker Compose** against the real database and bucket, deploy with `scp` + `docker build`, and narrow the IAM role to `PutObject`/`GetObject`.

**Why, and what would change my mind.** No extra cost, network paths and permissions already exist, and the load is trivial (one user, four calls every 5 minutes, 60 s cache). I would change course for **real users** (one VM in one zone cannot deliver 99.5 %), **any lasting public service** (needs HTTPS, authentication, rate limiting), **memory pressure**, or **frequent deployments by several people** (CI/CD and a managed service such as ECS on Fargate).

**Reboot (tested 2 Oct, 08:55 UTC).** After `sudo reboot`, the VM was back within about two minutes with no manual step: the `dashboard` container had restarted through its policy, `/health` answered with `model_loaded: true`, the 2 GB swap file was active again, and `docker` and `crond` were both `active`. A *stop/start* (not a reboot) changes the public IP.

**What Compose caught.** Nothing: it worked first time, confirming the image builds, the model loads and `.env` reaches RDS and S3. It could not catch what differed in the cloud: no security-group rule for port 8000 and the VM's narrower IAM role. Two Dockerfile defects (a `COPY` without a trailing `/`, Python 3.11 vs the 3.12 used for training) were found by reading it.

**Extends, not supersedes.** Nothing in ADR-004 was reversed. Added: a long-running container, port 8000, the narrowed IAM policy, swap as a written requirement.

**Consequences.** One VM now carries batch jobs and serving, so any VM failure takes everything down.

### ADR-006 — ML Serving Architecture (Day 4)

**Context.** Training data is the pipeline's exhaust: NO₂ rows from the database joined to traffic CSVs from S3. The traffic file for hour *HH* joins the NO₂ row stamped *HH+1*, because a Luchtmeetnet value averages the hour **ending** at its timestamp, while an NDW file is a snapshot taken **during** HH. Flagged rows are excluded; missing NO₂ hours are backfilled from the Luchtmeetnet API. The final run (2 Oct 2026, 07:59 UTC) has **25 joined hours** covering all 24 hours of the day (traffic 120–6,420 veh/h); the Day 4 model had 4. The live NDW feed only adds ~1 row per hour; NDW's historical database (Dexter) could supply more history, untested.

**Decision.** A **linear regression** on `total_intensity_veh_per_hr` (sum of four sites) and `hour_of_day` (UTC), saved as `model.pkl` (scikit-learn **1.8.0**, pinned) and **baked into the dashboard image**; `predict()` runs in-process. **Exceedance risk** = a sigmoid centred on **40 µg/m³**, steepness 0.2. If `predict()` fails, `/site/{id}` **degrades**: HTTP 200 with the real NO₂ and intensity, prediction fields `null` and a `prediction_error` message.

**Why linear, and what it learned.** *NO₂ = 28.57 − 0.00144 × traffic + 0.178 × hour.* In-sample R² is **0.08** and MAE **7.24 µg/m³**, against **7.77** for always predicting the mean; with under 30 rows there is no test set, so even that gain is optimistic. The traffic coefficient is **slightly negative**, opposite to the expected sign and to the 4-row model (+5.3 µg/m³ per 1,000 veh/h, plus a spurious −9.2 per hour that predicted 6.7 against 21.9 measured at midday). I do not read it as "traffic lowers NO₂". It is small, it has flipped once, and it is already negative in a traffic-only fit (−1.3 µg/m³ per 1,000 veh/h; correlation −0.25). The raw data show why: the busiest hour (15:00 UTC, 6,420 veh/h) had 15.5 µg/m³, while 07:00 UTC (2,880 veh/h) had 44.7 and 18:00 UTC (3,000 veh/h) had 40.2. Weather (daytime dilution, calm nights), the emission-to-measurement delay and a one-minute traffic snapshot compared with an hourly average are untested explanations. Also, hours 1–6 come only from 2 Oct and hours 7–23 only from 1 Oct, so hour of day cannot be separated from the weather of the day. **One day of snapshots does not predict NO₂ here**; predictions stay at ≈ 19–33 µg/m³. A flexible model would only fit that day's noise; readable coefficients exposed the problem. A complex model is worth testing only with weeks of data over weekdays, weekends and weather, weather features, and a time-based hold-out the linear model already beats the mean on.

**Why 40 µg/m³ and a sigmoid.** The EU hourly limit (200) is never near at NL10240 (11–45 observed), so risk would always be ≈ 0; the EU annual limit (40, falling to 20 from 2030) and WHO guidelines are averages, not hourly thresholds. 40 is the top of the "good" class for hourly NO₂ in the European air-quality classification. A logistic classifier was trained, but only 2 of 25 hours exceeded 40, so serving keeps regression + sigmoid, whose risk reads as "predicted value relative to the threshold". Recomputed on five real hours from `training_data.csv` with the settings of `train_model.py`, both gave low risks (0.04–0.12); for the one hour that truly exceeded 40 (07:00 UTC, 44.7 µg/m³) they gave 0.05 (regression) and 0.07 (logistic), and the logistic output over all rows (0.05–0.11) stays near the base rate of 2 in 25 (8 %). Neither detects exceedances. The steepness is a judgement, not learned, and risk never exceeds ≈ 0.2.

**Training-serving skew.** It would appear if serving computed features differently (one site's intensity instead of the four-site total, local time instead of UTC) or another scikit-learn version unpickled the model. Baking the model into the image makes **code, model and library versions one immutable unit**: no newer `model.pkl` can sit under code not written for it, nothing retrains live on unvalidated data, and rollback is "run the previous image".

**One station for four sites.** All four sites feed one interchange next to one measured target; nothing is estimated for a place without a sensor. A second interchange would need its own nearby station, because NO₂ falls steeply with distance from a road and depends on wind, and there would be no ground truth otherwise.

**Why degrade on failure.** Measured values are the trustworthy part; the prediction is the weakest. Failing the whole request would hide good data, and a 5xx would count against the SLO.

**Consequences.** One container, no model server, reproducible serving. Every retrain needs a rebuild and redeploy (done once: 4 → 25 rows). The API flags inputs outside the training range (`extrapolating`). The model should not be used for decisions, and the dashboard says so. Backlog: weeks of data, weather and cyclic-time features, a time-based hold-out.

---

## 3. Trade-off justifications

**Storage.** I considered DynamoDB, one relational database, and files only. I chose PostgreSQL for parsed readings and S3 for raw files: ~96,000 rows a year are tiny, structured and must be joined, and the raw feeds (~2.2 MB an hour, ~1.6 GB a month) are the only way to re-derive history after a parser change. I gave up simplicity (two stores, an RDS instance of ≈ €15 a month even when idle) and NoSQL's scale, which even 50 corridors (≈ 4.8 million rows a year) will not need.

**Compute.** I considered serverless functions, ECS on Fargate and a VM. I chose a t3.micro (≈ €10.60 a month) because it runs the existing containers unchanged for two sub-minute jobs an hour plus a one-user dashboard. I gave up availability (one VM, one zone, incompatible with 99.5 %), automatic patching and scaling, and memory headroom: 1 GiB needed a swap file, and 50 corridors would need ~40 minutes per hourly run unless the parser is rewritten.

**Messaging.** I considered direct writes, a Redis list and SNS → SQS. Day 2 used Redis to learn decoupling; from Day 3 the VM writes directly, because ~5 messages an hour and no consumers make a broker pure operational surface. I gave up adding consumers without touching producers and buffering during database outages; a second consumer brings back SNS → SQS.

**DR.** I weighed all four tiers against a 216-minute monthly budget. Backup & Restore (< $1 a month) cannot recover in time; Warm Standby (≈ $40) and Active-Active (≈ 2× production) buy minutes of recovery for hourly data. Pilot Light (≈ $12–15) meets an RTO of ≤ 1 hour. I gave up fast failover and accept a manual, untested runbook; traffic snapshots missed during an outage cannot be recovered from the live feed.

---

## 4. Why AWS — for the Municipality of Breda

*For a policy officer, not an engineer.*

AirBreda collects two kinds of public information every hour: air-quality measurements from the national monitoring network and traffic counts from the national traffic data service. It stores them, compares them and shows the result on a web page. We rent the computers, storage and database that do this from Amazon Web Services (AWS), one of the largest cloud providers.

**What AWS gives us.** We pay only for what we use, about €26 a month today, instead of buying and maintaining servers. AWS runs the database for us, keeps copies of the stored files, and lets us grow from one road junction to fifty without changing the design. Access is tightly controlled: the program that collects data may only add and read files in one storage area, and nothing else.

**Why it is appropriate for a Dutch public body.** All our data is stored inside the European Union, in AWS's Stockholm data centres, so European rules apply to how it is handled. The data is low-risk: it is already public and contains no personal information about residents. Still, AWS is an American company, and some governments worry that American law could give US authorities a claim on data held by US companies. For more sensitive future uses, AWS now offers a separate European Sovereign Cloud, located fully within the EU and separate from its other regions. The municipality should weigh this against its own information-security rules (the government baseline known as BIO) before scaling up.

**What we would lose by switching.** The programs are packaged in a portable way and would run elsewhere. Moving to another provider, for example Microsoft Azure, which has a data centre in the Netherlands, or a European provider, would mean rebuilding the access rules, database and storage setup: several days of specialist work, plus a period of running both systems side by side. We would also lose the documentation and experience built so far. The decision is reversible, but not free.

---

## 5. Cost estimate (AWS, eu-north-1, on-demand)

| Component | Current (1 corridor) | At 10 corridors | At 50 corridors |
|---|---|---|---|
| Compute (VM) | €10.59 | €17.45 | €32.00 |
| Database | €15.51 | €15.51 | €26.40 |
| Object storage | €0.22 | €0.33 | €0.84 |
| **Total** | **€26.32 / month** | **€33.30 / month** | **€59.24 / month** |

**Assumptions** (730 h a month; USD → EUR at 0.87; hourly ingestion; each extra corridor = 4 NDW sites and one more public Luchtmeetnet station):

- **Compute.** t3.micro $0.0108/h + public IPv4 $0.005/h + 8 GB gp3. At 10 corridors a **t3.small** ($0.0216/h) removes the memory risk; at 50, a **t3.medium** ($0.0432/h) + 20 GB.
- **Database.** db.t4g.micro ($0.016/h) + 20 GB (≈ $0.125/GB) + public IPv4. At 50 corridors db.t4g.small ($0.033/h), 50 GB, made private. *The instance class has not been checked in the RDS console; the estimate assumes db.t4g.micro.*
- **Object storage.** The raw feeds do not grow with corridors (~1.6 GB a month); ≈ 10 GB average in year one at ≈ $0.023/GB, plus requests (6, 42 or 202 objects an hour).
- **Not included:** data transfer, extra backups, the ADR-003 DR replica (+ ≈ €11–13), a load balancer, VAT.
- **Source note.** Prices are AWS list prices as republished by price trackers and AWS's IPv4 announcement; they have **not** been entered in calculator.aws.

**Is a single VM still right?** At **10 corridors**, yes: 40 sites take ≈ 8 minutes per run, well inside the hour. At **50**, it is borderline: 200 sites take ≈ 40 minutes unless the parser reads each feed once, one bigger VM is a single point of failure, and 5-minute ingestion would be impossible. ADR-005's threshold is then crossed: fix the parser, move the batch jobs to scheduled ECS Fargate tasks, and run the dashboard as two tasks behind a load balancer.

---

## 6. Reflection

**The decision I am least confident in** is `ON CONFLICT DO NOTHING`. It is right for what it was chosen for, duplicates from at-least-once ingestion, which it absorbed without error when two pipelines ran at once. But it quietly decides that the first value ever written is the permanent truth. If Luchtmeetnet replaces provisional values with validated ones (common for air-quality networks, but not something I have verified for this API), my database keeps the wrong number with no error and no log line. To become confident I would re-fetch a week of history daily and compare it with what is stored. If revisions exist, the rule should become `DO UPDATE` with a recorded validation status, so a correction can replace a provisional value but a bad value cannot overwrite a good one.

**With a full year of readings, the model would change in all three places.** *Evaluation* first: train on the first ten months, test on the last two, and report against the "predict the mean" baseline. With 25 rows the in-sample figures already showed the model barely beats the mean (MAE 7.24 vs 7.77) and could not settle even the sign of the traffic effect. *Features* next: a cyclic hour (sine and cosine), weekday versus weekend and, above all, weather: wind speed and direction, temperature and mixing height drive NO₂ at a fixed station at least as much as traffic. Traffic should become an hourly average of per-minute data, so both sides of the join describe the same hour; a year of history might come from NDW's Dexter instead of waiting. Only then the *algorithm*: gradient-boosted trees could capture effects such as wind direction relative to the motorway, but only if they beat the linear model on the held-out months, with exceedance risk learned by a calibrated classifier rather than an invented sigmoid steepness.

**The first thing I would add for a real municipal deployment is Infrastructure as Code with a CI/CD pipeline.** Today the system exists because of console clicks and commands typed over SSH: the security-group rules, IAM policy, swap file, crontab and `docker run` flags are documented here but reproducible only by someone who reads carefully. A Terraform or CloudFormation definition would make the environment reviewable, recreatable in the recovery region ADR-003 promises, and auditable. A pipeline that runs the 34 tests, builds versioned images (model version in the tag) and deploys them would replace `scp` and make every retrained model a traceable, reversible step. Close behind: alerting on a *missing* successful run (the silent memory kill showed error logs are not enough), a private database with its own least-privileged user, and HTTPS and authentication in front of the dashboard.

---

## Sources

- AWS, public IPv4 charge ($0.005 per IP per hour from 1 Feb 2024): <https://aws.amazon.com/blogs/aws/new-aws-public-ipv4-address-charge-public-ip-insights>
- AWS, European Sovereign Cloud general availability (15 Jan 2026): <https://press.aboutamazon.com/aws/2026/1/aws-launches-aws-european-sovereign-cloud-and-announces-expansion-across-europe>
- AWS Prescriptive Guidance, disaster recovery options (RPO/RTO per tier): <https://docs.aws.amazon.com/prescriptive-guidance/latest/strategy-database-disaster-recovery/defining.html>
- European Parliament Legislative Observatory, Directive (EU) 2024/2881 summary (NO₂ annual limit 40 → 20 µg/m³ from 2030): <https://oeil.europarl.europa.eu/oeil/en/document-summary?id=1796087>
- EEA Climate-ADAPT, hourly NO₂ classification ("good" < 40 µg/m³), EU and WHO values: <https://climate-adapt.eea.europa.eu/en/observatory/publications-data/analysis-data/cams-ground-level-no2-forecast>
- NDW, Dexter historical traffic database: <https://english.ndw.nu/products-and-services/dexter>; NDW documentation: <https://docs.ndw.nu/en/handleidingen/DEXTER/>
- Instance prices (republished AWS list prices): <https://www.doit.com/compute/spot/eu-north-1/t3.micro>, <https://sparecores.com/database/aws/db.t4g.micro>, <https://sparecores.com/database/aws/db.t4g.small>
