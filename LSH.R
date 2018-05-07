#Create MonetDB Connection
library(MonetDB.R)
library(DBI)
library(digest)
library(dplyr)
library(data.table)

# LSH Packages
library(LSHR)
library(Matrix)

library(doParallel)
library(mltools)


######## Fetch Data from DB ######## 
conn <- dbConnect(MonetDB.R(), host="localhost", dbname="demo", user="monetdb", password="monetdb")
diabetes_binary <- dbGetQuery(conn,"SELECT * FROM (SELECT *,
                              ROW_NUMBER() OVER (ORDER BY encounterid ASC) AS rownumber 
                              FROM MIMICIII.DIABETES_BINARY)  AS chunks")
dbDisconnect(conn)

# Transpose Data
diabetes.transpose <- setNames(data.frame(t(diabetes_binary[,-1])), diabetes_binary[,1])

######## Minhashing - Create a Signature Matrix ######## 
minHash <- function(diabetes.transpose, list_hashfct){
  sigMatrix <- data.frame(matrix(nrow=30,ncol=100097)) #matrix(,nrow=3000,ncol=100097)
  hashValue <- 0
  h_i <- 0
  
  # HashFunctions #
  hashFctCreator<-function(){
    a <- sample(1:1000, 1)
    b <- sample(1:1000, 1)
    p <- sample(1:1000, 1)
    func <- paste(a,"* x +",b,"%%",p)
    return(func)
  }
  
  list_hashfct <- vector("list", 30)
  for(i in 1:30){
    list_hashfct[i]<- hashFctCreator()
  }
  print("FUNCTIONS DONE")
  
  start_time <- Sys.time()
  total <- nrow(diabetes.transpose)
  
  #pb <- winProgressBar(title = "Minhashing Progress", min = 0,
   #                    max = total, width = 300)
  
  num_core <- detectCores() - 1  
  cl <- makeCluster(num_core, type='PSOCK')  
  registerDoParallel(cl)
  
  # Parallalization in Distance Calculation
  parLapply(cl, 1:nrow(diabetes.transpose), function(rowID){
    #for(rowID in 1:nrow(diabetes.transpose)){
    #setWinProgressBar(pb, rowID, title=paste(round(rowID/total*100, 0),"% done"))

    for(columnID in 1:ncol(diabetes.transpose)){
        if(diabetes.transpose[rowID, columnID] == 1){
          for(i in 1:30){
            h_i <- eval(bquote(function(x).(parse(text = list_hashfct[i])[[1]])))
            hashValue <- h_i(rowID)
            if(hashValue < sigMatrix[i,columnID] || sigMatrix[i,columnID] %in% NA){
              sigMatrix[i,columnID] <- unlist(hashValue)
            }
          } 
        }
    }
    }
  )
  stopCluster(cl)
  
  end_time <- Sys.time()
  time = end_time - start_time
  print(time)
  
  return(sigMatrix)
}

########## MAIN #########
signifMatrix <- minHash(diabetes.transpose)

########## LSH #######
lsh <- function(sigMatrix, bands, rows){
  for(i in seq(1, ncol(signifMatrix), bands)){
    for(band in i+1:i+bands){
      current <- signifMatrix[i,band]
      for(row in 1:rows){
        if(row == row){
          candidatepair(id1, id2)
        }
      }
    }
  }
}