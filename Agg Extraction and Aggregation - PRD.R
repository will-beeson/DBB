library(odbc)
library(RMySQL)
library(data.table)


## ---- Connect to PRD and pull bound policies ----

  con <- dbConnect(odbc(),
                   Driver = Sys.getenv("SQL_DRIVER"),
                   Server = Sys.getenv("PRD_SERVER"),
                   Database = Sys.getenv("PRD_DATABASE"),
                   UID = Sys.getenv("PRD_USERNAME"),
                   PWD      = Sys.getenv("PRD_PASSWORD"),
                   Port = 3306)
  
  
  query <- dbSendQuery(con, "SELECT `Policy Reference`, `Class of Business`, `Inception Date`, 
                       `Expiry Date`, `County`, `State`, `Limit Type`, `Group Rated Layer`, `Location Total Insured Value`,
                      `Group Total Insured Value`
                       FROM tableau WHERE `Policy Reference` IS NOT NULL " )
  
  data <- as.data.table(dbFetch(query))
  data[, `Inception Date` := as.Date(`Inception Date`, format = '%d/%m/%y')]
  data[, `Expiry Date` := as.Date(`Expiry Date`, format = '%d/%m/%y')] #format dates to remove time stamp 



## ---- Calculate bound aggregate for all locations, by limit type ---- 

  data[,`Location % of Group TIV` := `Location Total Insured Value`/`Group Total Insured Value`]
  
  data[`Limit Type` == "Per Location", `Bound Aggregate` := `Group Rated Layer` ]
  data[`Limit Type` != "Per Location", `Bound Aggregate` := `Group Rated Layer` * `Location % of Group TIV`]
  

## ---- Cycle through 1st of each month from April 2020 to present and calculate in-force for every location ----

  #set start and end dates as 1st April 2020 and first of the current month the code is being run. 
  start <- as.Date("01-04-20",format="%d-%m-%y")
  mth <- month(Sys.Date())
  yr <- year(Sys.Date()) - 2000 
  end <- as.Date(paste0("01-",mth,"-",yr),format="%d-%m-%y")
  
  
  months <- as.Date(seq(from = start, to=end ,by='months' ), format = '%d/%m/%y') #vector of all the in-force dates from start to end

  
  data[,"In_Force_Agg":= 0] #set up blank in-force column
  data[,"In_Force_Month":= months[1]] #set up intital in-force month column 
  agg_total <- data.table( `Class of Business` = character(), State = character(), County = character(), In_Force_Month = structure(integer(), class = 'Date'), In_Force_Agg = numeric())
  
  for ( i in seq_along(months))
  {
    data[,In_Force_Month:= months[i]] #set up the column to for in-force month
    data[, In_Force_Agg := pmin(1,pmax(0, as.numeric(In_Force_Month - `Inception Date` + 1)))] # in-force month is greater than or equal to the inception date indicator
    data[, In_Force_Agg := pmax(0, In_Force_Agg + pmin(1, pmax(0, as.numeric(`Expiry Date` - In_Force_Month ))) - 1)] # combined with expiry date, two truths = 2, less 1 and cap at bottom with 0
    data[, In_Force_Agg := In_Force_Agg * `Bound Aggregate`]
    agg <- data[,lapply(.SD,sum), by = .(`Class of Business`, State, County, In_Force_Month), .SDcols = c("In_Force_Agg")][order(`Class of Business`, State, County)]
    agg_total <- rbind(agg_total, agg)
    
    }

#write agg_total to .csv to be picked up by Tableau
  
  path <- paste0(Sys.getenv("DATA_DIR"),"In_Force_Agg_by_month.csv")
  write.csv(agg_total,path)
