-- 1) 전제 데이터 조회
SELECT *
FROM weather.processed_weather
LIMIT 20;

-- 2)날짜/도시별 평균 기온
SELECT date, city, AVG(temp_c) AS avg_temp
FROM weather.processed_weather
GROUP BY date, city
ORDER BY date, city;

-- 3) 날짜별 전국 평균 습도
SELECT date, AVG(humidity) AS avg_humidity
FROM weather.processed_weather
GROUP BY date
ORDER BY date;

-- 4) 특정 날짜(예: 2025-08-27) 도시별 요약
SELECT city,
       AVG(temp_c) AS avg_temp,
       MAX(precip_mm) AS max_precip,
       AVG(humidity) AS avg_humidity

FROM weather.processed_weather
WHERE date = "2025-08-27"
GROUP BY city
ORDER BY avg_temp DESC;

-- 5) 최근 7일간 일자별 평균기온 추이
SELECT date, AVG(temp_c) AS avg_temp
FROM weather.processed_weather
WHERE date BETWEEN '2025-08-20' AND '2025-08-27'
GROUP BY date
ORDER BY date;

