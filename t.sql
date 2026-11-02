SELECT is_hoax, COUNT(DISTINCT article) AS articles, COUNT(*) AS revisions
FROM revisions GROUP BY is_hoax;