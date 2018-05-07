#Create MonetDB Connection
library(MonetDB.R)
library(DBI)
library(digest)
library(dplyr)

#CPU
library(doParallel)
library(microbenchmark)

#For Binary CHanging
library(dummies)

#Distance Calculation
library(philentropy)

# datasetBool (1 = diabetes, 0 = patients)
main <- function(datasetBool, data){
  max<- nrow(data)
  dataFrame <- list()
  
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
      #calcDistanceFor(1,dataFrame,"gower","squared_chi")
      
      #TODO split table in 2 tables, join them afte binarizing them
      #df <- data.frame(matrix(unlist(dataFrame), nrow=50048),stringsAsFactors=FALSE)
      
      #allLevels <- levels(factor(unlist(df)))
      #do.call(cbind,lapply(df, function(x) table(sequence(nrow(df)), 
       #                                    factor(x, levels = allLevels))))
      
      #print("ATTENTION")
      #print(dataFrameBinary)
      calcDistanceFor(12,dataFrame,"sorensen","jaccard","cosine","kulczynski_d","czekanowski","mahalanobis")
    }
    
    if(datasetBool == 0){
      calcDistanceFor(0,dataFrame,"euclidean","manhattan","cosine","squared_euclidean","bhattacharyya","motyka","ruzicka","gower")
    }
  }
  end_time <- Sys.time()    
  print(string)
  print(end_time - start_time)
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
    #sqlStatement <- paste("SELECT * FROM (SELECT *,
     #                     ROW_NUMBER() OVER (ORDER BY encounter_id ASC) AS rownumber 
      #                    FROM mimiciii.diabetes)  AS chunks WHERE rownumber IN (",rowLines,")",sep="")
    sqlStatement <- paste("SELECT * FROM (SELECT *,
                          ROW_NUMBER() OVER (ORDER BY encounterid ASC) AS rownumber 
                          FROM mimiciii.diabetes_binary)  AS chunks WHERE rownumber IN (",rowLines,")",sep="")
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
    library(vegan)
    
    calc <- rbind(data[1,],data[row,])
    calc <- calc[-c(1, 2)]
    
    id_1 <- data[1,1]
    id_2 <- data[row,1]
    
    conn <- dbConnect(MonetDB.R(), host="localhost", dbname="demo", user="monetdb", password="monetdb")
    
    if(datasetBool == 1){
      distance1_calc <- distance(calc,method = distance1)
      distance2_calc <- distance(calc,method = distance2)
      
      sqlDistance <- paste("INSERT INTO mimiciii.diabetes_distances(encounter_id_1, encounter_id_2,",distance1,",","chi_squared",") 
                           VALUES (",id_1,",",id_2,",",distance1_calc,",",distance2_calc,")",sep="")
    }else if(datasetBool == 12){
      distance1_calc <- distance(calc,method = distance1)
      distance2_calc <- distance(calc,method = distance2)
      distance3_calc <- distance(calc,method = distance3)
      distance4_calc <- distance(calc,method = distance4)
      distance5_calc <- distance(calc,method = distance5)
      distance6_calc <- vegdist(calc,method = distance6,binary=TRUE, diag=FALSE, upper=FALSE)
      
      sqlDistance <- paste("INSERT INTO mimiciii.diabetes_binary_distances(encounter_ID_1, encounter_id_2,",distance1,",",distance2,", ",distance3,", ",distance4,", ",distance5,", ",distance6,") 
                           VALUES (",id_1,",",id_2,",",distance1_calc,",",distance2_calc,",",distance3_calc,",",distance4_calc,",",distance5_calc,",",distance6_calc,")",sep="")
    }else{
      distance1_calc <- distance(calc,method = distance1)
      distance2_calc <- distance(calc,method = distance2)
      distance3_calc <- distance(calc,method = distance3)
      distance4_calc <- distance(calc,method = distance4)
      distance5_calc <- distance(calc,method = distance5)
      distance6_calc <- distance(calc,method = distance6)
      distance7_calc <- distance(calc,method = distance7)
      distance8_calc <- distance(calc,method = distance8)
      
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
diabetes <- dbGetQuery(conn,"SELECT * FROM mimiciii.diabetes_binary")
mimic <- dbGetQuery(conn,"SELECT * FROM mimiciii.patients")
dbDisconnect(conn)

#main(0,mimic)
main(1, diabetes)