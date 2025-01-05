SELECT name AS NAME,count(*) AS NUM_APPEARANCE
FROM people AS p,crew AS c
WHERE p.person_id = c.person_id
GROUP BY p.person_id
ORDER BY NUM_APPEARANCE DESC
LIMIT 20;

--直接这样就好了，ORDER BY 可以用SELECT的名字。