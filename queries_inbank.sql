------------------------------A -----------------------------------------------
SELECT
    DC.Country,
    SUM(FCNM.MonthlyRevenue) AS TotalRevenue
FROM DW.FactCustomerNetflixMetrics FCNM
JOIN DW.DimCustomers DC ON FCNM.CustomerID = DC.CustomerID
GROUP BY DC.Country
ORDER BY TotalRevenue DESC
LIMIT 1;


-----------------------------B---------------------------------------------------
SELECT Country, SubscriptionType AS MostPopularPackage, PackageCount
FROM (
    SELECT
        DC.Country,
        DS.SubscriptionType,
        COUNT(*) AS PackageCount,
        ROW_NUMBER() OVER (PARTITION BY DC.Country ORDER BY COUNT(*) DESC) AS Ranking
    FROM DW.DimSubscription DS
    JOIN DW.DimCustomers DC ON DS.CustomerID = DC.CustomerID
    GROUP BY DC.Country, DS.SubscriptionType
) ranked
WHERE Ranking = 1;


----------------------------C-----------------------------------------------------

SELECT Country, COUNT(*) AS SubscriberCounthavingMoreActiveProfilesThenHousehold
FROM DW.DimCustomers DC
JOIN DW.FactCustomerNetflixMetrics FCM ON DC.CustomerID = FCM.CustomerID
WHERE FCM.ActiveProfiles > FCM.HouseholdProfileInd
GROUP BY Country
ORDER BY SubscriberCount DESC
LIMIT 1;

--  Another way to find it is, finding  the country having maximum difference between active profiles vs houshold 

SELECT Country, SUM(Difference) AS SumDifference
FROM (
    SELECT DC.Country, (FCM.ActiveProfiles - FCM.HouseholdProfileInd) AS Difference
    FROM DW.DimCustomers DC
    JOIN DW.FactCustomerNetflixMetrics FCM ON DC.CustomerID = FCM.CustomerID
) AS Differences
GROUP BY Country
ORDER BY SumDifference DESC
LIMIT 1;


----------------------------D--------------------------------------------------------

SELECT
    DC.Country,
    DC.YearOfBirth ,
    DS.SubscriptionType,
    SUM(FCM.MoviesWatched) AS TotalMoviesWatched,
    SUM(FCM.SeriesWatched) AS TotalSeriesWatched,
    DS.Device
FROM DW.FactCustomerNetflixMetrics FCM
JOIN DW.DimCustomers DC ON FCM.CustomerID = DC.CustomerID
JOIN DW.DimSubscription DS ON FCM.CustomerID = DS.CustomerID
GROUP BY DC.Country, DC.YearOfBirth, DS.SubscriptionType, DS.Device
ORDER BY DC.Country, DC.YearOfBirth, DS.SubscriptionType, DS.Device;

-- Another way of showing the segments as a period of 5 years

SELECT
    DC.Country,
    CONCAT(FLOOR((DC.YearOfBirth - 1) / 5) * 5, '-', FLOOR((DC.YearOfBirth - 1) / 5) * 5 + 4) AS CustomerSegmentPeriod,
    DS.SubscriptionType,
    SUM(FCM.MoviesWatched) AS TotalMoviesWatched,
    SUM(FCM.SeriesWatched) AS TotalSeriesWatched,
    DS.Device
FROM DW.FactCustomerNetflixMetrics FCM
JOIN DW.DimCustomers DC ON FCM.CustomerID = DC.CustomerID
JOIN DW.DimSubscription DS ON FCM.CustomerID = DS.CustomerID
GROUP BY DC.Country, CustomerSegmentPeriod, DS.SubscriptionType, DS.Device
ORDER BY DC.Country, CustomerSegmentPeriod, DS.SubscriptionType, DS.Device;
