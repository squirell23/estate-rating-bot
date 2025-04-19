CREATE TABLE building_ratings AS
WITH 
-- Образование
edu AS (
  SELECT b.building_id,
         LEAST(SUM(GREATEST(0, 10 * (1 - ST_Distance(b.geog, s.geog)::float/500))), 10) AS edu_score
  FROM building b
  JOIN school s ON ST_DWithin(b.geog, s.geog, 500)
  GROUP BY b.building_id
),
-- Детские сады
kind AS (
  SELECT b.building_id,
         LEAST(SUM(GREATEST(0, 10 * (1 - ST_Distance(b.geog, k.geog)::float/500))), 10) AS kind_score
  FROM building b
  JOIN kindergarten k ON ST_DWithin(b.geog, k.geog, 500)
  GROUP BY b.building_id
),
edu_total AS (
  SELECT b.building_id,
         LEAST(COALESCE(e.edu_score,0) + COALESCE(k.kind_score,0), 10) AS education_score
  FROM building b
  LEFT JOIN edu e ON b.building_id = e.building_id
  LEFT JOIN kind k ON b.building_id = k.building_id
),
-- Медицина
med AS (
  SELECT b.building_id,
         LEAST(SUM(GREATEST(0, 10 * (1 - ST_Distance(b.geog, h.geog)::float/500))), 10) AS med_score
  FROM building b
  JOIN hospital h ON ST_DWithin(b.geog, h.geog, 500)
  GROUP BY b.building_id
),
-- Парки
parks AS (
  SELECT b.building_id,
         LEAST(SUM(GREATEST(0, 5 * (1 - ST_Distance(b.geog, p.geog)::float/1000))), 5) AS park_score
  FROM building b
  JOIN park p ON ST_DWithin(b.geog, p.geog, 1000)
  GROUP BY b.building_id
),
  
diversity AS (
  SELECT b.building_id,
    CASE 
      WHEN ((CASE WHEN EXISTS (SELECT 1 FROM school s WHERE ST_DWithin(b.geog, s.geog, 500)) THEN 1 ELSE 0 END)
         + (CASE WHEN EXISTS (SELECT 1 FROM kindergarten k WHERE ST_DWithin(b.geog, k.geog, 500)) THEN 1 ELSE 0 END)
         + (CASE WHEN EXISTS (SELECT 1 FROM hospital h WHERE ST_DWithin(b.geog, h.geog, 500)) THEN 1 ELSE 0 END)
         + (CASE WHEN EXISTS (SELECT 1 FROM park p WHERE ST_DWithin(b.geog, p.geog, 1000)) THEN 1 ELSE 0 END)) = 4 THEN 5
      WHEN ((CASE WHEN EXISTS (SELECT 1 FROM school s WHERE ST_DWithin(b.geog, s.geog, 500)) THEN 1 ELSE 0 END)
         + (CASE WHEN EXISTS (SELECT 1 FROM kindergarten k WHERE ST_DWithin(b.geog, k.geog, 500)) THEN 1 ELSE 0 END)
         + (CASE WHEN EXISTS (SELECT 1 FROM hospital h WHERE ST_DWithin(b.geog, h.geog, 500)) THEN 1 ELSE 0 END)
         + (CASE WHEN EXISTS (SELECT 1 FROM park p WHERE ST_DWithin(b.geog, p.geog, 1000)) THEN 1 ELSE 0 END)) = 3 THEN 3
      WHEN ((CASE WHEN EXISTS (SELECT 1 FROM school s WHERE ST_DWithin(b.geog, s.geog, 500)) THEN 1 ELSE 0 END)
         + (CASE WHEN EXISTS (SELECT 1 FROM kindergarten k WHERE ST_DWithin(b.geog, k.geog, 500)) THEN 1 ELSE 0 END)
         + (CASE WHEN EXISTS (SELECT 1 FROM hospital h WHERE ST_DWithin(b.geog, h.geog, 500)) THEN 1 ELSE 0 END)
         + (CASE WHEN EXISTS (SELECT 1 FROM park p WHERE ST_DWithin(b.geog, p.geog, 1000)) THEN 1 ELSE 0 END)) = 2 THEN 1
      ELSE 0
    END AS diversity_bonus
  FROM building b
),
social AS (
  SELECT b.building_id,
         COALESCE(e.education_score,0) + COALESCE(m.med_score,0) + COALESCE(p.park_score,0) + COALESCE(d.diversity_bonus,0) AS social_score
  FROM building b
  LEFT JOIN edu_total e ON b.building_id = e.building_id
  LEFT JOIN med m ON b.building_id = m.building_id
  LEFT JOIN parks p ON b.building_id = p.building_id
  LEFT JOIN diversity d ON b.building_id = d.building_id
),
-- Качество недвижимости
quality AS (
  SELECT b.building_id,
         (CASE WHEN b.is_emergency THEN 0 ELSE 10 END) AS avariness_score,
         CASE 
           WHEN b.floors_number BETWEEN 3 AND 9 THEN 10
           WHEN b.floors_number < 3 THEN 10 * b.floors_number / 3.0
           WHEN b.floors_number > 9 THEN GREATEST(0, 10 - (b.floors_number - 9) * (10.0/6))
         END AS floors_score,
         CASE 
           WHEN (EXTRACT(YEAR FROM CURRENT_DATE) - b.build_year) <= 5 THEN 10
           WHEN (EXTRACT(YEAR FROM CURRENT_DATE) - b.build_year) >= 30 THEN 0
           ELSE 10 * (30 - (EXTRACT(YEAR FROM CURRENT_DATE) - b.build_year)) / 25.0
         END AS age_score
  FROM building b
),
quality_total AS (
  SELECT building_id,
         (avariness_score + floors_score + age_score) AS quality_score
  FROM quality
),
-- Транспортная доступность
transport AS (
  SELECT b.building_id,
         (
           SELECT CASE 
                    WHEN MIN(ST_Distance(b.geog, m.geog)) < 2400 THEN 10 * (1 - MIN(ST_Distance(b.geog, m.geog))/2400.0)
                    ELSE 0
                  END
           FROM metro m
           WHERE ST_DWithin(b.geog, m.geog, 5000)
         ) AS metro_score,
         (
           SELECT CASE 
                    WHEN COUNT(*) >= 5 THEN 10
                    ELSE 10 * COUNT(*) / 5.0
                  END
           FROM public_transport_stop pts
           WHERE ST_DWithin(b.geog, pts.geog, 1000)
         ) AS stops_score,
         (
           SELECT CASE 
                    WHEN ST_Distance(b.geog, ST_SetSRID(ST_MakePoint(37.617734,55.752004),4326)::geography) < 2000 THEN 10
                    WHEN ST_Distance(b.geog, ST_SetSRID(ST_MakePoint(37.617734,55.752004),4326)::geography) > 18000 THEN 0
                    ELSE 10 * (1 - (ST_Distance(b.geog, ST_SetSRID(ST_MakePoint(37.617734,55.752004),4326)::geography) - 2000) / 16000.0)
                  END
         ) AS center_score,
         (
           SELECT LEAST(SUM(
             CASE 
               WHEN ST_Distance(b.geog, p2.geog) < 500 
                 THEN LEAST((p2.car_capacity/50.0)*10, 10) * (1 - ST_Distance(b.geog, p2.geog)/500.0)
               ELSE 0 
             END
           ), 10)
           FROM parking p2
           WHERE ST_DWithin(b.geog, p2.geog, 500)
         ) AS parking_score
  FROM building b
),
transport_total AS (
  SELECT building_id,
         (metro_score + stops_score + center_score + parking_score) AS transport_score
  FROM transport
)
SELECT b.building_id,
       LEAST(s.social_score, 30) AS social_score,
       LEAST(qt.quality_score, 30) AS quality_score,
       LEAST(tt.transport_score, 40) AS transport_score,
       (LEAST(s.social_score, 30) + LEAST(qt.quality_score, 30) + LEAST(tt.transport_score, 40)) AS total_score
FROM building b
LEFT JOIN social s ON b.building_id = s.building_id
LEFT JOIN quality_total qt ON b.building_id = qt.building_id
LEFT JOIN transport_total tt ON b.building_id = tt.building_id;
