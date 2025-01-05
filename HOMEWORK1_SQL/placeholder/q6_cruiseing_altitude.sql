SELECT primary_title,votes
FROM ratings AS r,titles AS t,people AS p,crew AS c
WHERE p.name LIKE '%Cruise%'
    AND p.born = 1962
    AND p.person_id = c.person_id
    AND c.title_id = t.title_id
    AND r.title_id = t.title_id
ORDER BY votes DESC
LIMIT 10;


-- #投票在ratings中，人名在people，所以要把crew,people,ratings,titles全部都连接起来。
-- #然后满足题目条件之后在自然连接，最后排序。