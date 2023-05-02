SELECT LEFT(created_at, 10) AS date, ROUND((COUNT(CASE WHEN score >= 9 THEN 1 END) - COUNT(CASE WHEN score <= 6 THEN 1 END))::float*100 / COUNT(date), 2) AS nps
FROM gotjd709.nps
GROUP BY date
ORDER BY date