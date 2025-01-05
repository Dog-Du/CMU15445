--法一：不使用CASE语句：

-- SELECT name,2022-born as age
-- FROM people
-- WHERE born >= 1900 AND died IS NULL
-- union
-- SELECT name,died-born as age
-- FROM people
-- WHERE born >= 1900 AND died IS NOT NULL
-- ORDER BY age DESC,name
-- LIMIT 20;

--按照年龄和名字的复合排序，因为不会CASE WHEN
--所以使用 union 代替 或 的含义，相当于代替了if的条件分支。s


--法二，使用CASE语句：
SELECT name,
    CASE 
        WHEN died IS NOT NULL
            THEN died - born
        ELSE
            2022 - born
    END
AS age
FROM people
WHERE born >= 1900
ORDER BY age  DESC ,name
LIMIT 20; 