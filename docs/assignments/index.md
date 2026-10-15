# Assignments: overview & grading

There are five individual assignments and one group capstone. Each assignment is released after the relevant chapters and gives students about four days in an intensive block (or two weeks in a semester).

| # | Title | Chapters | Primary data | Weight |
|---|---|---|---|---|
| [A1](a1-formats-engines.md) | Formats & single-node engines | 1–3, 5 | NYC TLC trips | 10% |
| [A2](a2-spark.md) | MapReduce & Spark at scale | 4, 6, 7 | Wikipedia pageviews | 10% |
| [A3](a3-ml.md) | Machine learning at scale | 8 | NYC TLC or Amazon Reviews | 10% |
| [A4](a4-streaming.md) | Streaming analytics | 9 | Wikimedia EventStreams | 10% |
| [A5](a5-geospatial.md) | Geospatial big data for Rwanda | 10 | CHIRPS, Open Buildings, WorldPop | 10% |
| [Capstone](capstone.md) | Group project | All | Student choice | 40% |

Assignments count for 40% in total: the best four of five, unless the instructor announces otherwise.

## Submission format

Every submission is a **GitHub repository** (private, shared with the instructor, or via GitHub Classroom) containing:

```text
README.md            # how to run, what each file does, summary of findings
requirements.txt     # or environment.yml, with versions
notebooks/           # exploratory notebooks, executed, outputs visible
src/                 # reusable code (functions, jobs)
report.pdf           # 3–6 pages: method, results, discussion (or report.md)
data/README.md       # how to download the data — NOT the data itself
```

## Common rubric

| Criterion | Excellent (90–100%) | Good (70–89%) | Adequate (50–69%) | Insufficient (<50%) |
|---|---|---|---|---|
| **Correctness (35%)** | All tasks correct, results validated against independent checks | Minor errors | Some tasks incorrect or unvalidated | Major errors or missing tasks |
| **Scalability reasoning (25%)** | Choices justified with measurements (time, memory, plans, Spark UI); trade-offs discussed | Sensible choices, some measurement | Works but no justification | Approach would not scale; no awareness |
| **Code quality & reproducibility (20%)** | Clean, modular, documented; runs from README in one go | Mostly reproducible | Hard to run; hard-coded paths | Cannot be run |
| **Communication (20%)** | Clear report, good figures, honest limitations, actionable conclusions | Clear but thin discussion | Results without interpretation | Unclear or missing report |

## Use of AI assistants

AI assistants may be used for help with syntax, debugging and explanations. Students must (1) state in the README what they used them for, (2) be able to explain every line of code in an oral check, and (3) never submit generated text as their own analysis without verification. Results that cannot be reproduced score zero for correctness.

## Late policy and compute problems

Power cuts and connectivity problems are real. If a dataset download or cloud job fails, document what happened and submit results on a documented subset; partial credit is given for sound reasoning on smaller data. Contact the instructor or tutors before the deadline, not after.
