#Create MonetDB Connection
library(MonetDB.R)
library(DBI)
library(digest)
library(dplyr)

#CPU
library(doParallel)
library(microbenchmark)

#Distance Calculation
library(philentropy)

# TODO: LSH measure time for cosine (read, apply (min cosine, more like jaccard adapt data))

# datasetBool (1 = diabetes, 0 = patients)
main <- function(datasetBool, data){
  max<- nrow(data)
  dataFrame <- list()
  id <- 1
  
  #while(id != 8){
    start_time <- Sys.time()
    for(rowID in 1:max){
      conn <- dbConnect(MonetDB.R(), host="localhost", dbname="demo", user="monetdb", password="monetdb")
      #Dataset has following Number of Rows
      rowIDsFromChunking <- getChunksFrom(rowID, max)
      
      #Create Data Frame
      dataFrame <- getDataFrameFromDataset(datasetBool, rowIDsFromChunking)
      dbDisconnect(conn)
      
      #Calculate Distance
      print("Calculation...")
      if(datasetBool == 1){
        # calcDistanceFor(1,dataFrame,string)
      }
      
      if(datasetBool == 0){
        calcDistanceFor(0,dataFrame,"euclidean","manhattan","cosine","squared_euclidean","bhattacharyya","motyka","ruzicka","gower")
      }
    }
    end_time <- Sys.time()    
    print(string)
    print(end_time - start_time)
  #}
  #print(time_euclidean)
  #print(time_manhatten)
  #print(time_cosine)
  #print(time_squared_euclidean)
  #print(time_bhattacharyya)
  #print(time_motyka)
  #print(time_ruzicka)
  #pritn(time_gower)
}

# Create array with row numbers for chuncking/baching and put in dataframe
getChunksFrom <- function(rowID, max){
  compNumOfVec = (max-1)/2;
  
  tableRows <- c()
  id <- 1;
  joinID <- 1
  
  if (rowID < max/2) {
    joinID = rowID + 1;
    
    while((joinID <= (rowID + (max/2))) && (id  <= compNumOfVec)) {
      tableRows <- c(tableRows, joinID);
      joinID <- joinID + 1;
      id <- id + 1;
    }
  } else if(rowID == max) {
    while(isTRUE((joinID < max/2) && (id  <= compNumOfVec))){
      tableRows <- c(tableRows, joinID);
      joinID <- joinID + 1;
      id <- id + 1;
    }
  } else {
    while(((id  <= compNumOfVec) && (joinID < (max/2 - (max - rowID)%%(max/2))))){
      tableRows <- c(tableRows, joinID);
      joinID <- joinID + 1;
      id <- id + 1;
    }
    
    joinID <- rowID + 1;
    
    while((id  <= compNumOfVec) &&((joinID > rowID) && (joinID <= max))){
      tableRows <- c(tableRows, joinID);
      joinID <- joinID + 1;
      id <- id + 1;
    }
  }
  print(length(tableRows))
  
  return(tableRows)
}

getDataFrameFromDataset <- function(datasetBool, rowList){
  rowLines <- toString(rowList)
  
  if(datasetBool == 1){
    sqlStatement <- paste("SELECT * FROM (SELECT *,
                        ROW_NUMBER() OVER (ORDER BY encounter_id ASC) AS rownumber 
                          FROM mimiciii.diabetes)  AS chunks WHERE rownumber IN (",rowLines,")",sep="")
  } else{
    sqlStatement <- paste("SELECT * FROM (SELECT *,
                        ROW_NUMBER() OVER (ORDER BY icustay_id ASC) AS rownumber 
                          FROM mimiciii.patients)  AS chunks WHERE rownumber IN (",rowLines,")",sep="")
  }
  datasetDataFrame <- dbGetQuery(conn,sqlStatement)
  return(datasetDataFrame)
}

############################## Distance Calculation #############################
calcDistanceFor <- function(datasetBool, data, distance1,distance2,distance3,distance4,distance5,distance6,distance7,distance8){
  num_core <- detectCores() - 1  
  cl <- makeCluster(num_core, type='PSOCK')  
  registerDoParallel(cl)
  
  # Parallalization in Distance Calculation
  parLapply(cl, 2:nrow(data), function(row){
  #for(row in 2:nrow(data)){
    library(MonetDB.R)
    library(DBI)
    library(digest)
    library(dplyr)
    library(philentropy)
  
    calc <- rbind(data[1,],data[row,])
    calc <- calc[-c(1, 2)]

    distance1_calc <- distance(calc,method = distance1)
    distance2_calc <- distance(calc,method = distance2)
    distance3_calc <- distance(calc,method = distance3)
    distance4_calc <- distance(calc,method = distance4)
    distance5_calc <- distance(calc,method = distance5)
    distance6_calc <- distance(calc,method = distance6)
    distance7_calc <- distance(calc,method = distance7)
    distance8_calc <- distance(calc,method = distance8)
    
    print(distance1_calc)
    print(distance2_calc)
    print(distance3_calc)
    print(distance4_calc)
    print(distance5_calc)
    print(distance6_calc)
    print(distance7_calc)
    print(distance8_calc)
    
    id_1 <- data[1,1]
    id_2 <- data[row,1]
    
    conn <- dbConnect(MonetDB.R(), host="localhost", dbname="demo", user="monetdb", password="monetdb")
    
    if(datasetBool == 1){
      sqlDistance <- paste("INSERT INTO mimiciii.diabetes_distances(encounter_id_1, encounter_id_2,",distance,") 
                        VALUES (",id_1,",",id_2,",",distance_calc,")",sep="")
    } else{
      if(is.nan(distance5_calc)){
        distance5_calc <- 0
      }
      sqlDistance <- paste("INSERT INTO mimiciii.patient_distances(icustay_id_1, icustay_id_2,"
                    ,distance1,", ",distance2,", ",distance3,", ",distance4,", ",distance5,", ",distance6,", ",distance7,", ",distance8,")  
                    VALUES (",id_1,",",id_2,",",distance1_calc,",",distance2_calc,",",distance3_calc,",",distance4_calc,",",distance5_calc,",",distance6_calc,",",distance7_calc,",",distance8_calc,")",sep="")
    }
    dbGetQuery(conn,sqlDistance)
    dbDisconnect(conn)
    }
  )  
  stopCluster(cl)
}

###################### EXECUTION: Connect to MonetDB Server ######################
conn <- dbConnect(MonetDB.R(), host="localhost", dbname="demo", user="monetdb", password="monetdb")
diabetes <- dbGetQuery(conn,"SELECT * FROM mimiciii.diabetes")
mimic <- dbGetQuery(conn,"SELECT * FROM mimiciii.patients")
dbDisconnect(conn)

main(0, mimic)