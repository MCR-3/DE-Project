# Landing keeps the whole source scope; matching FSA to Ofsted schools belongs to staging

The project only cares about the food premises of schools, yet the FSA landing fetches every English establishment (about 500,000) instead of only those that match an Ofsted school. FSA and Ofsted share no identifier, so matching them is fuzzy work (postcode, name, business type) that will be wrong at first and need reruns; and the FSA API only exposes the current state, so anything a run does not save is lost for that date. Landing therefore stays a faithful copy of the source and the matching happens in staging, where it can be corrected and replayed over the whole history.

To keep that volume reasonable, a full FSA run happens weekly (Mondays, 02:00 UTC) rather than daily: ratings change slowly (an inspection every six months to two years), and a daily full run would add about 500,000 documents and 400 MB of files a day.

## Considered Options

- **Scrape FSA only for the Ofsted schools** (one API search per school): about 22,000 calls instead of about 2,600, and the result depends on a matching rule frozen at scrape time. Rejected.
- **Filter on the FSA business type `School/college/university`**: a source-side filter, so acceptable in principle, but school kitchens are often registered as `Other catering premises`, `Restaurant/Cafe/Canteen` or `Caring Premises` (breakfast and after-school clubs), which would be lost. Rejected.

## Consequences

- `fsa_landing` and `ofsted_landing` stay independent: neither waits for the other.
- Small tests use sample runs (`sample: true`, a few local authorities). They land in the same collections as full runs, so staging must leave out every document with `sample: true`.
