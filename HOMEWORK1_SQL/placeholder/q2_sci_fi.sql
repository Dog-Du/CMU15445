SELECT primary_title,premiered,runtime_minutes || " (mins)"
FROM titles
WHERE genres LIKE '%Sci-Fi%'
ORDER BY runtime_minutes DESC
LIMIT 10;


--偶嘎西纳，如果在runtime_minutes后面加上 as runtime 就过不了了，不知道为什么.
