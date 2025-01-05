SELECT COUNT(DISTINCT title_id)
FROM titles AS t
WHERE t.premiered = 
    (SELECT premiered FROM titles WHERE primary_title = "Army of Thieves");

--这个简单一点。筛选出来年代一样的，然后COUNT的时候把title_id给DISTINCT一下。