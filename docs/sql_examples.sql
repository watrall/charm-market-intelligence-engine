-- Skill counts (top 25)
SELECT s.skill, COUNT(*) AS n
FROM job_skills s
JOIN jobs j ON j.id = s.job_id
GROUP BY 1
ORDER BY 2 DESC
LIMIT 25;

-- Recent Section 106 / NEPA jobs
SELECT j.title, j.company, j.location, j.date_posted, j.job_url
FROM jobs j
JOIN job_skills s ON s.job_id = j.id
WHERE s.skill IN ('Section 106','NEPA')
  AND (j.date_posted IS NOT NULL OR j.created_at >= datetime('now','-90 days'))
ORDER BY j.date_posted DESC, j.created_at DESC;

-- Salary summary by skill (where available)
SELECT s.skill,
       COUNT(*) AS n,
       ROUND(AVG(COALESCE(j.salary_min, j.salary_max)), 2) AS avg_low,
       ROUND(AVG(COALESCE(j.salary_max, j.salary_min)), 2) AS avg_high
FROM jobs j
JOIN job_skills s ON s.job_id = j.id
WHERE j.salary_min IS NOT NULL OR j.salary_max IS NOT NULL
GROUP BY s.skill
ORDER BY n DESC
LIMIT 20;
