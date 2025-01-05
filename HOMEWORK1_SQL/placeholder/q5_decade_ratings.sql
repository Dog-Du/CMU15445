SELECT CAST(premiered/10*10 AS TEXT) || 's' AS DECADE,
    ROUND(AVG(rating),2) AS AVG_RATING,
    MAX(rating) AS TOP_RATING,
    MIN(rating) AS MIN_RATING,
    COUNT(*)    AS NUM_RELEASES
FROM titles AS t, ratings AS r
WHERE t.title_id = r.title_id and premiered IS NOT NULL
GROUP BY DECADE
ORDER BY AVG_RATING DESC, DECADE ASC;

-- #因为字符串才能拼接，所以要把年代转化成字符串，也就是 CAST( **** AS TEXT) 之后再 || 's'
-- #premiered改成年代，先除以10，再乘以10即可
-- #注意premiered不能为空