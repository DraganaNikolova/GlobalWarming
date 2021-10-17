create table Country
(
	id int identity primary key,
	name nvarchar(100)
)

create table City
(
	id int identity primary key,
	name nvarchar(100),
	country int references Country(id),
	lat float,
	lon float
)

create table CO2
(
	id int identity primary key,
	year int,
	co2 float,
	country int references Country(id)
)

create table DailyAverageTemperatures
(
	id int identity primary key,
	timestamp date,
	city int,
	average float,
	minimum float,
	maximum float
)

create table TemperatureAnomaly
(
	id int identity primary key,
	timestamp date,
	anomaly float,
	city int
)

create table TemperatureMonthly_1750_1820
(
	id int identity primary key,
	timestamp date,
	city int references City(id),
	temperature float
)

create table TemperatureMonthly_1820_1890
(
	id int identity primary key,
	timestamp date,
	city int references City(id),
	temperature float
)

create table TemperatureMonthly_1890_1960
(
	id int identity primary key,
	timestamp date,
	city int references City(id),
	temperature float
)

create table TemperatureMonthly_1960_2030
(
	id int identity primary key,
	timestamp date,
	city int references City(id),
	temperature float
)

create table TemperaturePartitions
(
	name varchar(100) primary key,
	start_time datetime,
	end_time datetime
)

create or alter procedure InsertMonthlyAverageTemperature
as
    declare @Timestamp datetime = (select CURRENT_TIMESTAMP)
    declare @Month int = ((select datepart(month, @Timestamp)) - 1)
    declare @Year int = ((select datepart(year, @Timestamp)))
    declare @Date datetime = convert(datetime, cast((concat(@Year, '-', @Month, '-', '01' )) as date))

    declare @sourceTableVariable nvarchar(50);
    set @sourceTableVariable = (select top 1 name from TemperaturePartitions order by start_time desc);

    declare @dynamicSQL nvarchar(1000);
    declare @ParmDefinition nvarchar(500);
begin

    -- inserting data into the last partitioned table
    SET @dynamicSQL =
             N'insert into ' + @sourceTableVariable + '
               (timestamp, city, temperature) select CONVERT(date, @timestamp), city, avg(average)
		        from DailyAverageTemperatures
                where timestamp >= @timestamp and timestamp < dateadd(month, 1, @timestamp)
                group by city';
        SET @ParmDefinition = N'@timestamp datetime';
        -- execute the string with the parameter value
        EXECUTE sp_executesql @dynamicSQL, @ParmDefinition,
                              @timestamp = @Date;

end
go

create or alter procedure insertTemperature @Day datetime, @City nvarchar(100), @Temperature float,
@Minimum float, @Maximum float
as
    declare @CityId int = (select id from City where name = @City)
begin

    insert into DailyAverageTemperatures(timestamp, city, average, minimum, maximum)
    select CONVERT(date, @Day), @CityId, @Temperature, @Minimum, @Maximum

end;
go
 
create or alter procedure getCities
as
begin

    select distinct name as City
    from City

end;
go

create or alter procedure getDate @DateTime datetime
as
begin

    select CONVERT(date, @DateTime)

end;
go

create table Continent
(
    id int identity primary key,
    name nvarchar(100)
)

alter table Country
	add continent int
go

alter table Country
	add constraint Country_Continent_id_fk
		foreign key (continent) references Continent
go

--
--
-- FUNCTIONALITIES
--
--

-- 1) Get the warmest decade for a country and city

create or alter procedure warmestDecade @Country nvarchar(100), @City nvarchar(100)
as
    declare @tableName varchar(255);
    declare @dynamicSQL nvarchar(500);
    declare @paramDefinition nvarchar(100);
	create table #TemperatureByYear
    (
        average float,
        year int
    );
begin
        -- select all partitions
        select name
		into #tableNames
        from TemperaturePartitions

        declare TableNameCursor cursor for select name from #tableNames

        open TableNameCursor
        fetch next from TableNameCursor into @tableName

        -- loop each partition
        while @@fetch_status = 0
        begin
            set @dynamicSQL =
                'select avg(temperature), year(timestamp)
                from ' + @tableName + ' t join City c1 on t.city = c1.id join Country c2 on c2.id = c1.country
                where c2.name = @country and c1.name = @city
                group by year(timestamp)'
            set @paramDefinition = N'@country nvarchar(100), @city nvarchar(100)';
			-- execute the string with the defined parameters
            insert into #TemperatureByYear
			execute sp_executesql @dynamicSQL, @paramDefinition,
								  @country = @Country, @city = @City;

            fetch next from TableNameCursor into @tableName
        end
        close TableNameCursor
        deallocate TableNameCursor

        -- group by 10 years and find average temperature
        select min(year) as start, max(year) as last, avg(average) as average
        into #TemperatureByDecade
        from #TemperatureByYear
        group by year/10*10;

        -- select the decade with maximum average temperature
        select t.average, concat(t.start, '-', t.last) as range
        from #TemperatureByDecade t
        where t.average = (select max(average) from #TemperatureByDecade);
end
go


-- 2) Get the average temperature year value for country and city, where functionality is
-- max - warmest on min - coldest
create or alter procedure yearCalculations @Country nvarchar(100), @City nvarchar(100), @Functionality varchar(3)
as
    declare @tableName varchar(255);
    declare @dynamicSQL nvarchar(500);
    declare @paramDefinition nvarchar(100);
	create table #TemperatureByYear
    (
        average float,
        year int
    );
begin
        -- select all partitions
        select name
		into #tableNames
        from TemperaturePartitions

        declare TableNameCursor cursor for select name from #tableNames

        open TableNameCursor
        fetch next from TableNameCursor into @tableName

        -- loop each partition
        while @@fetch_status = 0
        begin
            set @dynamicSQL =
                'select avg(temperature), year(timestamp)
                from ' + @tableName + ' t join City c1 on t.city = c1.id join Country c2 on c2.id = c1.country
                where c2.name = @country and c1.name = @city
                group by year(timestamp)'
            set @paramDefinition = N'@country nvarchar(100), @city nvarchar(100)';
			-- execute the string with the defined parameters
            insert into #TemperatureByYear
			execute sp_executesql @dynamicSQL, @paramDefinition,
								  @country = @Country, @city = @City;

            fetch next from TableNameCursor into @tableName
        end
        close TableNameCursor
        deallocate TableNameCursor

        if @Functionality = 'max'
        begin
            -- get the year with maximum average temperature
            select average, year
            from #TemperatureByYear
            where average = (select max(average) from #TemperatureByYear)
        end
        else if @Functionality = 'min'
        begin
            -- get the year with minimum average temperature
            select average, year
            from #TemperatureByYear
            where average = (select min(average) from #TemperatureByYear)
        end
end
go


-- 3) Get the average month value for country and city, where functionality is
-- max - warmest on min - coldest

create or alter procedure monthCalculations @Country nvarchar(100), @City nvarchar(100), @Functionality varchar(3)
as
    declare @tableName varchar(255);
    declare @dynamicSQL nvarchar(500);
    declare @paramDefinition nvarchar(100);
	create table #TemperatureByYear
    (
        average float,
        year int,
        month int
    );
begin
        -- select all partitions
        select name
		into #tableNames
        from TemperaturePartitions

        declare TableNameCursor cursor for select name from #tableNames

        open TableNameCursor
        fetch next from TableNameCursor into @tableName

        -- loop each partition
        while @@fetch_status = 0
        begin
            set @dynamicSQL =
                'select avg(temperature), year(timestamp), month(timestamp)
                from ' + @tableName + ' t join City c1 on t.city = c1.id join Country c2 on c2.id = c1.country
                where c2.name = @country and c1.name = @city
                group by year(timestamp), month(timestamp)'
            set @paramDefinition = N'@country nvarchar(100), @city nvarchar(100)';
			-- execute the string with the defined parameters
            insert into #TemperatureByYear
			execute sp_executesql @dynamicSQL, @paramDefinition,
								  @country = @Country, @city = @City;

            fetch next from TableNameCursor into @tableName
        end
        close TableNameCursor
        deallocate TableNameCursor

        if @Functionality = 'max'
        begin
            -- get the month with maximum average temperature
            select average, year, month, datename(month ,dateadd(month, month, -1 ))
            from #TemperatureByYear
            where average = (select max(average) from #TemperatureByYear)
        end
        else if @Functionality = 'min'
        begin
            -- get the month with maximum average temperature
            select average, year, month, datename(month ,dateadd(month, month, -1 ))
            from #TemperatureByYear
            where average = (select min(average) from #TemperatureByYear)
        end
end
go


-- 4) Get the biggest anomalies increases for country, city and the chosen time range

create or alter procedure maxAnomalyPerYear @Country nvarchar(100), @City nvarchar(100),
                                           @StartDate datetime, @EndDate datetime
as
begin

    -- get all year between @StartDate and @EndDate
    with years as
    (
        select  year(@StartDate) as year
        union all
        select  year + 1
        from    years
        where   year < year(@EndDate)
    )
    -- get the anomalies for each month in the years between
    select anomaly, year(timestamp) as year, month(timestamp) as month
    into #AnomalyByMonth
    from TemperatureAnomaly t join City c1 on t.city = c1.id join Country c2 on c2.id = c1.country
    where c2.name = @country and c1.name = @city and year(timestamp) in (select year from years)

    select year, max(anomaly) as anomaly
    into #MaxAnomalyPerYear
    from #AnomalyByMonth
    group by year

    -- get the maximum month anomaly per each year
    select t2.anomaly, t1.year, month, datename(month ,dateadd(month, month, -1 ))
    from #AnomalyByMonth t1 join #MaxAnomalyPerYear t2 on t1.year = t2.year
    where t1.anomaly = t2.anomaly
end
go


-- 5) Get 10 warmest years for country

create or alter procedure warmest10Years @Country nvarchar(100), @City nvarchar(100)
as
    declare @tableName varchar(255);
    declare @dynamicSQL nvarchar(500);
    declare @paramDefinition nvarchar(100);
	create table #TemperatureByYear
    (
        average float,
        year int
    );
begin
        -- select all partitions
        select name
		into #tableNames
        from TemperaturePartitions

        declare TableNameCursor cursor for select name from #tableNames

        open TableNameCursor
        fetch next from TableNameCursor into @tableName

        -- loop each partition
        while @@fetch_status = 0
        begin
            set @dynamicSQL =
                'select avg(temperature), year(timestamp)
                from ' + @tableName + ' t join City c1 on t.city = c1.id join Country c2 on c2.id = c1.country
                where c2.name = @country and c1.name = @city
                group by year(timestamp)'
            set @paramDefinition = N'@country nvarchar(100), @city nvarchar(100)';
			-- execute the string with the defined parameters
            insert into #TemperatureByYear
			execute sp_executesql @dynamicSQL, @paramDefinition,
								  @country = @Country, @city = @City;

            fetch next from TableNameCursor into @tableName
        end
        close TableNameCursor
        deallocate TableNameCursor

        -- get top 10 years with maximum average temperature
        select top 10 average, year
        from #TemperatureByYear
        order by average desc
end
go


-- 6) Get average temperature monthly values between a start and end point for city

create or alter procedure getFilteredTemperatures @StartDate datetime, @EndDate datetime, @Country nvarchar(100), @City nvarchar(100)
as
    set @StartDate = cast(@StartDate as date)
    set @EndDate = cast(@EndDate as date)
    declare @CityId int = (select top 1 C1.id
                           from City C1 join Country C2 on C1.country = C2.id
                           where C1.name = @City and C2.name = @Country)

    declare @tableName varchar(255)
	declare @dynamicSQL nvarchar(500);
    declare @ParamDefinition nvarchar(100);

begin

        -- find the partitions based on the time range
        select name
		into #tableNames
        from TemperaturePartitions
        where (@startDate >= start_time and @startDate <= end_time) or
              (@startDate <= start_time and @endDate > end_time) or
              (@endDate > start_time and @endDate <= end_time)

        declare TableNameCursor cursor for select name from #tableNames

        open TableNameCursor
        fetch next from TableNameCursor into @tableName
        while @@fetch_status = 0
        begin

           -- delete any previous result
           delete from ResultChartTemperature;

           -- insert the result in a table which Ignition will use
           SET @dynamicSQL =
				 N'insert into ResultChartTemperature(temperature, time)
				   select temperature, timestamp
				   from ' + @tableName + '
				   where timestamp >= @start and timestamp < @end and city = @city';
			SET @ParamDefinition = N'@start datetime, @end datetime, @city int';
			-- execute the string with the defined parameters
			EXECUTE sp_executesql @dynamicSQL, @ParamDefinition,
								  @start = @StartDate, @end = @EndDate, @city  = @CityId ;

            fetch next from TableNameCursor into @tableName
        end
        close TableNameCursor
        deallocate TableNameCursor

end;
go


-- 7) Get CO2 values by year between a start and end point for city 

create or alter procedure getFilteredCO2 @StartDate datetime, @EndDate datetime, @Country nvarchar(100)
as
    set @StartDate = cast(@StartDate as date)
    set @EndDate = cast(@EndDate as date)
    declare @CountryId int = (select top 1 C1.id
                              from Country C1
                              where C1.name = @Country )


begin

    -- delete any previous result
    delete from ResultChartCO2;

    -- insert the result in a table which Ignition will use
    insert into ResultChartCO2(time, co2)
    select datefromparts(year, 1, 1), convert(decimal(10, 2),(co2/1000000)) as co2
    from CO2
    where @StartDate <= datefromparts(year, 1, 1) and @EndDate >= datefromparts(year, 1, 1) and
          country = @CountryId;


end;
go


-- 8) Get highest CO2 emission in megaton (Mton)
-- 1 Mton = 1000000 ton

create or alter procedure highestCO2 @Country nvarchar(100)
as
    declare @MaxCO2 float = (select max(co2) as co2
                            from CO2 C1 join Country C2 on C1.country = C2.id
                            where C2.name = @Country
                            group by name)
begin

    select convert(decimal(10, 2),(co2/1000000)) as co2, C1.year, C2.name
    from CO2 C1 join Country C2 on C1.country = C2.id
    where C2.name = @Country and C1.co2 = @MaxCo2

end;
go


-- 9) Get world co2 emission in tones for the latest year

create or alter procedure worldLatestCO2
as
begin

    select top 1 C2.name, co2
    from CO2 C1 join Country C2 on C2.id = C1.country
    where year = (select max(year) from CO2)
    order by co2 desc

end;
go

exec worldLatestCO2


-- 10) CO2 ordered emissions in tonnes by countries for the latest year

create or alter procedure highestCO2Countries @Order varchar(4)
as

begin

    select row_number() over (order by co2 desc) AS Row_Counter, C2.name as country, co2
    from CO2 C1 join Country C2 on C2.id = C1.country
    where year = (select max(year) from CO2) and C2.id in (select country from City)
    order by
        case when @Order = 'asc' then co2 end,
        case when @Order = 'desc' then co2 end desc;

end;
go


-- 11) Top 20 biggest CO2 emissions by countries for the latest year

create or alter procedure top20CO2Countries
as
    declare @WorldCor2 float = (select co2 from CO2 C join Country C1 on C1.id = C.country
                                where C1.name = 'World' and year = (select max(year) from CO2))
begin

    select top 20 C2.name as country, C3.id, C3.name, (co2/@WorldCor2)*100 as percentage
    from CO2 C1 join Country C2 on C2.id = C1.country join Continent C3 on C2.continent = C3.id
    where year = (select max(year) from CO2) and C2.id in (select country from City)
    order by co2 desc
end;
go


-- 12) Monthly global temperatures from 1744 to 2020

create or alter procedure globalMeanTemperatures
as
    declare @tableName varchar(255);
    declare @dynamicSQL nvarchar(500);
	create table #GlobalMonthMean
    (
        average float,
        month int,
        year int
    );
begin
        -- select all partitions
        select name
		into #tableNames
        from TemperaturePartitions

        declare TableNameCursor cursor for select name from #tableNames

        open TableNameCursor
        fetch next from TableNameCursor into @tableName

        -- loop each partition
        while @@fetch_status = 0
        begin
            set @dynamicSQL =
                'select avg(temperature), month(timestamp), year(timestamp)
                from ' + @tableName + '
                where year(timestamp) != 1743
                group by month(timestamp), year(timestamp)'
			-- execute the string with the defined parameters
            insert into #GlobalMonthMean
			execute sp_executesql @dynamicSQL

            fetch next from TableNameCursor into @tableName
        end
        close TableNameCursor
        deallocate TableNameCursor

        -- get average global temperature by month for every year
        select average, year, month, left(datename(month ,dateadd(month, month, -1 )), 3)
        from #GlobalMonthMean
        order by year, month

end
go


-- 13) Monthly global mean anomalies from 1750 to 2020

create or alter procedure globalMeanAnomalies
as
begin
        select convert(decimal(10, 2),avg(anomaly)), year(timestamp), month(timestamp),
               left(datename(month ,dateadd(month, month(timestamp), -1 )), 3)
        from TemperatureAnomaly
        group by year(timestamp), month(timestamp)
        order by year(timestamp), month(timestamp)
end
go


-- 14) Last week's weather news for a city

create or alter procedure lastWeekNews @Country nvarchar(100), @City nvarchar(100), @EndDate datetime
as
begin
    select distinct minimum, maximum, average, timestamp, datename(weekday, timestamp) as day
    from DailyAverageTemperatures t join City C1 on t.city = C1.id join Country C2 on C2.id = C1.country
    where C1.name = @City and C2.name = @Country and
          timestamp >= dateadd(day, -7, cast(@EndDate as date)) and
          timestamp < cast(@EndDate as date)
    order by timestamp
end
go


-- 15) Get the percentage increase or decrease in average temperature from last week weather

create or alter procedure differenceLastWeek @Country nvarchar(100), @City nvarchar(100), @EndDate datetime
as
    -- average temperature for current week
    declare @averageTemp float = (select avg(average)
                                  from DailyAverageTemperatures t join City C1 on t.city = C1.id join Country C2 on C2.id = C1.country
                                  where C1.name = @City and C2.name = @Country and
                                        timestamp >= dateadd(day, -7, cast(@EndDate as date)) and
                                        timestamp < cast(@EndDate as date))
    -- average temperature for last week
    declare @averageTempLastWeek float = (select avg(average)
                                          from DailyAverageTemperatures t join City C1 on t.city = C1.id join Country C2 on C2.id = C1.country
                                          where C1.name = @City and C2.name = @Country and
                                                timestamp >= dateadd(day, -14, cast(@EndDate as date)) and
                                                timestamp < dateadd(day, -7, cast(@EndDate as date)))
    -- change = difference/original * 100
    declare @Percentage float = (((@averageTemp-@averageTempLastWeek)/@averageTempLastWeek)*100)
begin

    select @Percentage

end
go


-- 16) Get temperature monthly anomalies between a start and end point for city

create   procedure getAnomaliesForCountry @Country nvarchar(100), @City nvarchar(100), @Start date, @End date
as
begin

    delete from ResultChartAnomalies;

    insert into ResultChartAnomalies(anomaly, time)
    select anomaly, timestamp as time
    from TemperatureAnomaly T join City C on T.city = C.id join Country C2 on C2.id = C.country
    where C.name = @City and C2.name = @Country and timestamp >= @Start and timestamp <= @End
end;
go
