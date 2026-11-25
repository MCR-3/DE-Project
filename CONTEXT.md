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

**Fetch date**:
The moment a run received a piece of data from the source.
_Avoid_: Extract date, scrape date, ingestion date

**Published date**:
The date the source gives to the version of the data a run received: for FSA, the last time the establishment's local authority published its data; for Ofsted, the "as at" date of the release. Empty when the source gives none.
_Avoid_: Extract date, release date, update date

**Authority queue**:
The local authorities a FSA run still has to fetch, largest first (by number of establishments), shared by the run's Fetchers.
_Avoid_: Job queue, task list

**Fetcher**:
One of the parallel units of a FSA run that takes one authority at a time from the Authority queue and saves all of its pages, resuming from the last saved page if the authority was interrupted.
_Avoid_: Worker (reserved for Airflow's Celery workers), consumer, scraper

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
