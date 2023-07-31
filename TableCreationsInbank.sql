 -- Table Creations
 
--------------------- Load table-----------------------
CREATE DATABASE inbank;
USE inbank;
CREATE SCHEMA DWLoad;

CREATE TABLE DWLoad.NetflixSubscription (
	UserId	INT ,
    SubscriptionType VARCHAR(20),
    MonthlyRevenue DECIMAL(10, 2),
    JoinDate DATE,
    LastPaymentDate DATE,
    Country VARCHAR(50),
    Age INT,
    Gender VARCHAR(10),
    Device VARCHAR(20),
    PlanDuration VARCHAR(20),
    ActiveProfiles INT,
    HouseholdProfileInd INT,
    MoviesWatched INT,
    SeriesWatched INT,
    RecordDate DATE
);


select * from NetflixSubscription limit 10000 ;


----------------------Stage tables--------------------
CREATE SCHEMA DWStage;

CREATE TABLE DWStage.DimCustomers (
	CustomerID INT NOT NULL,
    Country VARCHAR(50),
    Gender VARCHAR(10),
    YearOfBirth INT,
	JoiningDate DATE,
    RecordDate DATE
);

CREATE TABLE DWStage.DimSubscription(
	CustomerID INT NOT NULL,
	SubscriptionDuration VARCHAR(20),
	SubscriptionType VARCHAR(20),
    JoiningDate DATE,
    Device VARCHAR(20),
    RecordDate DATE
);

CREATE TABLE DWStage.FactCustomerNetflixMetrics (
    CustomerID INT NOT NULL,
    MonthlyRevenue DECIMAL(10, 2),
    ActiveProfiles INT,
    HouseholdProfileInd INT,
    MoviesWatched INT,
    SeriesWatched INT,
    LastPaymentDate DATE,
    RecordDate DATE
);

--------------------- CNF tables --------------------
CREATE SCHEMA CNF;

CREATE TABLE CNF.DimDeviceType(
	DeviceTypeKey INT AUTO_INCREMENT,
	DeviceType varchar(20)
);

CREATE TABLE CNF.DimSubscriptionType (
    SubscriptionTypeKey INT PRIMARY KEY AUTO_INCREMENT,
    SubscriptionType VARCHAR(20)
);

CREATE TABLE DimDate (
    date_key INT NOT NULL,
    date DATE NULL,
    year INT NULL,
    quarter INT NULL,
    month VARCHAR(20) NULL,
    week INT NULL,
    day_of_week VARCHAR(20) NULL,
    day_of_month INT NULL,
    PRIMARY KEY (date_key)
);


-------------------------SKey TABLE------------------
CREATE SCHEMA DWSkey;

CREATE TABLE DWSkey.DimCustomers (
	RecordKey INT PRIMARY KEY AUTO_INCREMENT,
    CustomerID INT,
    Country VARCHAR(50),
    Gender VARCHAR(10),
    YearOfBirth INT,
	JoiningDate DATE,
	RecordDate DATE,
	ValidFromDate DATE,
    ValidToDateKey DATE,
    IsCurrent BOOLEAN,
	
	-- Add unique constraints
    CONSTRAINT uc_DWSkey_DimCustomers UNIQUE(CustomerID,RecordDate),

    -- Add indexing
    INDEX idx_RecordKey (RecordKey),
    INDEX idx_CustomerID (CustomerID)
	
);
CREATE TABLE DWSkey.DimSubscription(
	RecordKey INT PRIMARY KEY AUTO_INCREMENT,
	CustomerID INT NOT NULL,
	SubscriptionDuration VARCHAR(20),
	SubscriptionType VARCHAR(20),
    JoiningDate DATE,
    Device VARCHAR(20),
    RecordDate DATE,
	ValidFromDate DATE,
    ValidToDateKey DATE,
    IsCurrent BOOLEAN,
	
	-- Add unique constraints
    CONSTRAINT uc_DWSkey_DimSubscription UNIQUE(CustomerID,RecordDate),

    -- Add indexing
    INDEX idx_RecordKey (RecordKey),
    INDEX idx_CustomerID (CustomerID)
	
);

----------------------Conform -----------------------------------
CREATE SCHEMA DWConform;

CREATE TABLE DWConform.DimCustomers (
    CustomerID INT NOT NULL,
    Country VARCHAR(50) NOT NULL,
	Gender VARCHAR(10) NOT NULL,
    YearOfBirth INT NOT NULL,
	JoiningDate DATE NOT NULL,
	JoiningDateID INT NOT NULL,
	RecordDate DATE NOT NULL,
	RecordDateKey INT NOT NULL,
	ValidFromDate DATE NOT NULL,
    ValidToDate DATE NOT NULL	
);

CREATE TABLE DWConform.DimSubscription(
	CustomerID INT NOT NULL,
	SubscriptionDuration VARCHAR(20) NOT NULL,
	SubscriptionType VARCHAR(20)  NOT NULL,
	SubscriptionTypeID INT NOT NULL,
    Device VARCHAR(20) NOT NULL,	
    DeviceID INT NOT NULL,
    JoiningDate DATE NOT NULL,
	JoiningDateID INT NOT NULL,
	RecordDate DATE NOT NULL,
	RecordDateKey INT NOT NULL,
	ValidFromDate DATE NOT NULL,
    ValidToDate DATE NOT NULL
	
);

CREATE TABLE DWConform.FactCustomerNetflixMetrics (
    CustomerID INT NOT NULL,
    MonthlyRevenue DECIMAL(10, 2) NOT NULL,
    ActiveProfiles INT NOT NULL,
    HouseholdProfileInd INT NOT NULL,
    MoviesWatched INT NOT NULL,
    SeriesWatched INT NOT NULL,
    LastPaymentDate DATE NOT NULL,
	LastPaymentDateID INT NOT NULL,
    RecordDate DATE NOT NULL,
	RecordDateKey INT NOT NULL
);

-----------------------DW tables ------------------------
CREATE SCHEMA DW

CREATE TABLE DW.DimCustomers (
    CustomerID INT NOT NULL Primary Key,
    Country VARCHAR(50) NOT NULL,
    Gender VARCHAR(10) NOT NULL,
    YearOfBirth INT NOT NULL,
    JoiningDate DATE NOT NULL,
    JoiningDateID INT NOT NULL,
    RecordDate DATE NOT NULL,
    RecordDateKey INT NOT NULL,
    ValidFromDate DATE NOT NULL,
    ValidToDate DATE NOT NULL,
    INDEX idx_CustomerID (CustomerID),   -- Index on CustomerID
    UNIQUE KEY uc_DW_DimCustomers (CustomerID,ValidFromDate) -- Unique constraint on CustomerID and RecordDateKey
);

CREATE TABLE DW.DimSubscription (
    CustomerID INT NOT NULL Primary Key,
    SubscriptionDuration VARCHAR(20) NOT NULL,
    SubscriptionType VARCHAR(20) NOT NULL,
    SubscriptionTypeID INT NOT NULL,
    Device VARCHAR(20) NOT NULL,
    DeviceID INT NOT NULL,
    JoiningDate DATE NOT NULL,
    JoiningDateID INT NOT NULL,
    RecordDate DATE NOT NULL,
    RecordDateKey INT NOT NULL,
    ValidFromDate DATE NOT NULL,
    ValidToDate DATE NOT NULL,
    INDEX idx_CustomerID (CustomerID),   -- Index on CustomerID
    UNIQUE KEY uc_DW_DimSubscription (CustomerID,ValidFromDate) -- Unique constraint on CustomerID and RecordDateKey
);

CREATE TABLE DW.FactCustomerNetflixMetrics (
    CustomerID INT NOT NULL,
    MonthlyRevenue DECIMAL(10, 2) NOT NULL,
    ActiveProfiles INT NOT NULL,
    HouseholdProfileInd INT NOT NULL,
    MoviesWatched INT NOT NULL,
    SeriesWatched INT NOT NULL,
    LastPaymentDate DATE NOT NULL,
	LastPaymentDateID INT NOT NULL,
    RecordDate DATE NOT NULL,
	RecordDateKey INT NOT NULL,
    INDEX idx_CustomerID (CustomerID),   -- Index on CustomerID
	PRIMARY KEY pk_DW_FactCustomerNetflixMetrics(CustomerID,LastPaymentDateID,RecordDateKey) -- Primary Key Constraint on CustomerID,LastPaymentDateID,RecordDateKey
);

 