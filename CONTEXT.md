# DE-Project

Collects UK food hygiene ratings (FSA) and school inspection results (Ofsted) so that the hygiene of food premises can be related to schools. Today only the landing zone exists: the sources' data stored as received.

## Language

### Runs

**Run**:
One execution of a landing DAG, identified by its run id (the logical date, `YYYYMMDDTHHMMSS`), whose data lands in its own folder and is tagged with that id in MongoDB.
_Avoid_: Job, batch, load

**Full run**:
A run that covers the whole of the source's scope (for FSA, every English local authority).
_Avoid_: Complete extract, all

**Sample run**:
A run limited to a small, chosen subset of the source (for FSA, a few local authorities), made for testing and never to be read as the state of the whole source.
_Avoid_: Extract, test extract, subset

**Extract date**:
The date at which the source itself produced the data a run received (FSA `meta.extractDate`, the Ofsted release date), as opposed to when the run happened.
_Avoid_: Scrape date, ingestion date

### FSA food hygiene

**Rating**:
The overall food hygiene result of an establishment: 0 to 5 under the FHRS scheme (England, Wales, Northern Ireland), or Pass / Improvement Required under the FHIS scheme (Scotland), plus statuses such as Awaiting Inspection or Exempt.
_Avoid_: Overall score, grade, note globale

**Score**:
One of the three penalty points an inspection gives an establishment (Hygiene, Structural, Confidence in management), where 0 is best; the parts behind a Rating.
_Avoid_: Rating, sub-rating

**Score descriptor**:
The FSA's fixed wording for one Score value in one category (for example Hygiene 15 = "Improvement necessary").
_Avoid_: Score label, score description
