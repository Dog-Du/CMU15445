
--法一：
--先找出来Nicole Kidman这个人，然后找出来其他的演员，最后让他们两个的title_id相等，
--这个时候左边都是Nicole后面就是工作的同事们了。


-- SELECT DISTINCT t2.name
-- FROM (SELECT title_id,crew.person_id,name 
--         FROM crew,people 
--         WHERE people.person_id = crew.person_id 
--         AND people.name = "Nicole Kidman" 
--         AND people.born = 1967) AS t1,
--     (SELECT title_id,crew.person_id,name 
--     FROM crew,people 
--     WHERE people.person_id = crew.person_id
--     AND (crew.category = "actor" OR crew.category = "actress")) AS t2
-- WHERE t1.title_id = t2.title_id 
-- ORDER BY t2.name;


--i法二：使用With As语句

With 
Nicole AS(
    SELECT title_id
    FROM crew,people
    WHERE people.person_id = crew.person_id
    AND people.name = "Nicole Kidman" 
    AND people.born = 1967
),
Other As(
    SELECT title_id,name
    FROM crew,people
    WHERE crew.person_id = people.person_id
    AND (crew.category = "actor" OR crew.category = "actress")
)
SELECT DISTINCT name
FROM Nicole,Other
WHERE Nicole.title_id = Other.title_id
ORDER BY name;