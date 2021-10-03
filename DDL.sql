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



