--法一：没有使用CTE，用了函数。
-- WITH special_movie AS (
-- SELECT titles.title_id AS title_id
-- FROM titles
-- WHERE primary_title="House of the Dragon" AND type = "tvSeries"),
-- name_seq AS(
-- SELECT DISTINCT(" " || akas.title) AS title
-- FROM akas INNER JOIN special_movie ON akas.title_id=special_movie.title_id
-- ORDER BY akas.title ASC)
-- SELECT LTrim(GROUP_CONCAT(title))
-- FROM name_seq;


WITH 
special_movie AS(
    SELECT 
        DISTINCT(akas.title) AS title, 
        titles.primary_title AS name
    FROM 
        akas INNER JOIN titles 
            ON akas.title_id =titles.title_id
    WHERE titles.primary_title = 'House of the Dragon' 
        AND titles.type = 'tvSeries'
    ORDER BY akas.title),--–找到符合要求的所有名称，已去重，已排序
num AS(
SELECT row_number() OVER (ORDER BY special_movie.name ASC) AS number, special_movie.title as title
FROM special_movie ),--–给每个名称编号

-- –-number title
-- –-1 A Casa do Dragão
-- –-2 A Guerra dos Tronos: A Casa do Dragão
-- -–3 Dom smoka…
function_cte AS(
    SELECT number, title
    FROM num
    WHERE number=1
    UNION
    SELECT num.number,function_cte.title || ', ' || num.title
    FROM num 
        JOIN function_cte 
            ON num.number=function_cte.number+1)
SELECT title
FROM function_cte
ORDER BY number DESC
LIMIT 1;