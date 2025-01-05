WITH 
actor_movie_1955 AS( --找到1955年生的人演的所有电影，同时保留name,persion_id,title_id,primary_title
    SELECT
        people.person_id, -- 根据人分组
        people.name,
        titles.title_id, -- 用来查找ratings
        titles.primary_title
    FROM 
        people,crew,titles
    WHERE people.person_id = crew.person_id
        AND titles.title_id = crew.title_id
        AND people.born = 1955 
        AND titles.type = "movie"
),
actor_ratings AS( --把那些电影和人按照人来分组，因为我们的结果是和人相关的，把评分和电影关联，求得平均分数和名字
    SELECT 
        name,
        ROUND(AVG(ratings.rating),2) AS rating
    FROM ratings,actor_movie_1955
    WHERE ratings.title_id = actor_movie_1955.title_id
    GROUP BY actor_movie_1955.person_id
),
quartiles AS ( --
    SELECT *,NTILE(10) OVER (ORDER BY rating ASC)
        AS RatingQuartile FROM actor_ratings
)
SELECT name,rating
FROM quartiles
WHERE RatingQuartile = 9
ORDER BY rating DESC,name ASC;


--NTILE()函数，就是把所有行为n等分。
--思路：先找1955年生的人，然后根据人来分组，列出作品，算出平均值，最后用NTILE来分桶，然后给出第九份。