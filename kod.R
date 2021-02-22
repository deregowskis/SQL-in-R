options(stringsAsFactors = FALSE,message=FALSE)

library(sqldf)
library(dplyr)
library(data.table)

Badges <- read.csv("Badges.csv")
Tags <- read.csv("Tags.csv")
Posts <- read.csv("Posts.csv")
PostLinks <- read.csv("PostLinks.csv")
Comments <- read.csv("Comments.csv")
Users <- read.csv("Users.csv")
Votes <- read.csv("Votes.csv")

### Praca projektowa nr 2
### Rozwi¹zanie zadan
### Sebastian Derêgowski 305701 kierunek IAD

## ------------------------ Zadanie 1 ----------------------------

## ---- sqldf ----

df_sql_1 <- function(Posts){sqldf('
  SELECT Title, Score, ViewCount, FavoriteCount 
  FROM Posts 
  WHERE PostTypeId = 1 AND FavoriteCount>=25 AND ViewCount>=10000')}

## ---- bazowy ----

df_base_1 <- function(Posts){
  Posts[Posts$FavoriteCount>=25 & Posts$PostTypeId==1 & Posts$ViewCount>=10000,c("Title","Score","ViewCount","FavoriteCount")]}

## ---- dplyr ----

df_dplyr_1 <- function(Posts){
  data <- filter(Posts, PostTypeId == 1, FavoriteCount>=25, ViewCount >=10000);
  select(data, Title, Score, ViewCount, FavoriteCount)}

## ---- data.table ----

df_table_1 <- function(Posts){
  data <- as.data.table(Posts);
  data[PostTypeId == 1 & FavoriteCount>=25 & ViewCount >=10000,.(Title,Score,ViewCount,FavoriteCount)]}

## ------------------------ Zadanie 2 ----------------------------

## ---- sqldf ----

df_sql_2 <- function(Tags,Posts,Users){sqldf('
  SELECT Tags.TagName, Tags.Count, Posts.OwnerUserId, Users.Age, Users.Location, Users.DisplayName 
  FROM Tags 
  JOIN Posts ON Posts.Id=Tags.WikiPostId 
  JOIN Users ON Users.AccountId=Posts.OwnerUserId 
  WHERE OwnerUserId!=-1
  ORDER BY Count DESC')}

## ---- bazowy ----

df_base_2 <- function(Tags,Posts,Users){
  data <- merge(Tags,Posts,by.x="WikiPostId",by.y="Id");
  data<-merge(data,Users,by.x="OwnerUserId",by.y="AccountId");
  data<-data[data$OwnerUserId != -1 & !(is.na(data$OwnerUserId)), c("TagName","Count","OwnerUserId","Age","Location","DisplayName"),];
  data[order(data$Count,decreasing=TRUE),]}

## ---- dplyr ----

df_dplyr_2 <- function(Tags,Posts,Users){
  data <- inner_join(Tags,Posts,by = c("WikiPostId" = "Id"));
  data <- inner_join(data,Users,by=c("OwnerUserId"="AccountId"));
  data <- arrange(data,desc(Count));
  data <- filter(data, OwnerUserId != -1);
  select(data, TagName,Count,OwnerUserId,Age,Location,DisplayName)}

## ---- data.table ----

df_table_2 <- function(Tags,Posts,Users){
  tagsdata <- as.data.table(Tags);
  postsdata <- as.data.table(Posts);
  usersdata <- as.data.table(Users);
  data <- merge(tagsdata,postsdata,by.x="WikiPostId",by.y="Id");
  data <- merge(data,usersdata,by.x="OwnerUserId",by.y="AccountId");
  data <- data[OwnerUserId != -1,.(TagName,Count,OwnerUserId,Age,Location,DisplayName)];
  setorderv(x=data, cols="Count",order=-1L);
  data}

## ------------------------ Zadanie 3 ----------------------------

## ---- sqldf ----

df_sql_3 <- function(PostLinks,Posts){sqldf('
  SELECT Posts.Title, RelatedTab.NumLinks 
  FROM (
        SELECT RelatedPostId AS PostId, COUNT(*) AS NumLinks 
        FROM PostLinks 
        GROUP BY RelatedPostId) AS RelatedTab 
  JOIN Posts ON RelatedTab.PostId=Posts.Id 
  WHERE Posts.PostTypeId=1 
  ORDER BY Numlinks DESC')}

## ---- bazowy ----

df_base_3 <- function(PostLinks,Posts){
  RelatedTab <- aggregate(x = PostLinks$RelatedPostId,by = PostLinks["RelatedPostId"],FUN=length);
  colnames(RelatedTab)[1] <- "PostId";
  colnames(RelatedTab)[2] <- "NumLinks";
  data <- Posts[Posts$PostTypeId == 1,];
  data <- merge(RelatedTab,data,by.x="PostId",by.y="Id");
  data <- data[order(data$NumLink,decreasing=TRUE),];
  data[,c("Title","NumLinks")]
}

## ---- dplyr ----

df_dplyr_3 <- function(PostLinks,Posts){
  RelatedTab <- PostLinks%>%
    group_by(PostId=RelatedPostId)%>%
    summarize(NumLinks=n())
  RelatedTab <- inner_join(RelatedTab,Posts,by = c("PostId" = "Id"));
  RelatedTab <- filter(RelatedTab, PostTypeId == 1);
  RelatedTab <- arrange(RelatedTab,desc(NumLinks))
  select(RelatedTab,Title,NumLinks)
}

## ---- data.table ----

df_table_3 <- function(PostLinks,Posts){
  postlinksdata <- data.table(PostLinks)
  postsdata <- Posts
  RelatedTab <- postlinksdata[,.(.N),by=.(RelatedPostId)][postsdata,on=c(RelatedPostId="Id"),nomatch=NULL][PostTypeId==1,.(Title,"NumLinks"=N)][order(-NumLinks)]
}

## ------------------------ Zadanie 4 ----------------------------

## ---- sqldf ----

df_sql_4 <- function(Users,Badges){sqldf('
  SELECT DISTINCT Users.Id, Users.DisplayName, Users.Reputation, Users.Age, Users.Location 
  FROM (
  SELECT Name, UserID 
  FROM Badges 
  WHERE Name IN (
    SELECT Name 
    FROM Badges 
    WHERE Class=1 
    GROUP BY Name HAVING COUNT(*) BETWEEN 2 AND 10) 
  AND Class=1) AS ValuableBadges 
  JOIN Users ON ValuableBadges.UserId=Users.Id')}

## ---- bazowy ----

df_base_4 <- function(Users,Badges){
  x <- as.data.frame(table(Badges[Badges$Class==1,"Name"]),stringsAsFactors = FALSE)
  x <- x[x$Freq>=2 & x$Freq<=10,]
  colnames(x)[1] <- "Name"
  x <- x[,"Name"]
  ValuableBadges <- Badges[Badges$Class==1 & Badges$Name %in% x,]
  ValuableBadges <- ValuableBadges[,c("Name","UserId")]
  outputdata <- merge(Users,ValuableBadges,by.x="Id",by.y = "UserId")
  unique(outputdata[,c("Id","DisplayName","Reputation","Age","Location")])
}

## ---- dplyr ----

df_dplyr_4 <- function(Users,Badges){
  namescolumn <- filter(Badges,Class==1)
  namescolumn <- group_by(namescolumn,Name)
  namescolumn <- summarize(namescolumn,NumLinks=n())
  namescolumn <- filter(namescolumn,NumLinks>=2 & NumLinks<=10)
  namescolumn <- select(namescolumn, -NumLinks)
  vector <- pull(namescolumn, Name)
  ValuableBadges <- filter(Badges,Class==1 & Name%in%vector)
  ValuableBadges <- select(ValuableBadges, Name, UserId)
  outputdata <- inner_join(Users,ValuableBadges,by = c("Id" = "UserId"))
  distinct(outputdata, Id, DisplayName, Reputation, Age, Location)
}

## ---- data.table ----

df_table_4 <- function(Users,Badges){
  usersdata <- as.data.table(Users)
  badgesdata <- as.data.table(Badges)
  data <- badgesdata[badgesdata$Class==1,.(.N),by=.(Name)]
  data <- data[data$N>=2 & data$N<=10,Name]
  ValuableBadges <- badgesdata[badgesdata$Name %in% data & badgesdata$Class ==1, .(Name, UserId)]
  setkey(usersdata,"Id")
  setkey(ValuableBadges,"UserId")
  data <- usersdata[ValuableBadges,nomatch=0]
  unique(data[,.(Id, DisplayName, Reputation, Age, Location)])
}

## ------------------------ Zadanie 5 ----------------------------

## ---- sqldf ----

df_sql_5 <- function(Votes){sqldf('
  SELECT UpVotesTab.PostId, UpVotesTab.UpVotes, IFNULL(DownVotesTab.DownVotes, 0) AS DownVotes 
  FROM (
    SELECT PostId, COUNT(*) AS UpVotes 
    FROM Votes 
    WHERE VoteTypeId=2 
    GROUP BY PostId) AS UpVotesTab 
  LEFT JOIN (
    SELECT PostId, COUNT(*) AS DownVotes 
    FROM Votes 
    WHERE VoteTypeId=3 
    GROUP BY PostID) AS DownVotesTab 
  ON UpVotesTab.PostId=DownVotesTab.PostId')}

## ---- bazowy ----

df_base_5 <-function(Votes){
  UpVotesTab <- Votes[Votes$VoteTypeId==2, "PostId", drop=FALSE]
  UpVotesTab <- aggregate(UpVotesTab$PostId, UpVotesTab["PostId"], length)
  colnames(UpVotesTab)[2] <- "UpVotes"
  DownVotesTab <- Votes[Votes$VoteTypeId==3, "PostId", drop=FALSE]
  DownVotesTab <- aggregate(DownVotesTab$PostId, DownVotesTab["PostId"], length)
  colnames(DownVotesTab)[2] <- "DownVotes"
  output <- merge(x = UpVotesTab, y = DownVotesTab,
                  by ='PostId', all.x=TRUE, all.y = FALSE)
  output$DownVotes[is.na(output$DownVotes)] <- 0
  output$UpVotes[is.na(output$UpVotes)] <- 0
  output[,c("PostId","UpVotes","DownVotes")]
}

## ---- dplyr ----

df_dplyr_5 <- function(Votes){
  UpVotesTab <- filter(Votes,VoteTypeId==2)
  UpVotesTab <- group_by(UpVotesTab,PostId)
  UpVotesTab <- summarize(UpVotesTab,UpVotes=n())
  DownVotesTab <- filter(Votes,VoteTypeId==3)
  DownVotesTab <- group_by(DownVotesTab,PostId)
  DownVotesTab <- summarize(DownVotesTab,DownVotes=n())
  output <- left_join(UpVotesTab,DownVotesTab,by="PostId")
  output$DownVotes <- coalesce(output$DownVotes,0L)
  output$UpVotes <- coalesce(output$UpVotes,0L)
  select(output, PostId, UpVotes, DownVotes)
}

## ---- data.table ----

df_table_5 <- function(Votes){
  votesdata <- as.data.table(Votes)
  UpVotes <- votesdata[votesdata$VoteTypeId==2,.N,by= .(PostId)]
  setnames(UpVotes,c("PostId","N"),c("PostId","UpVotes"))
  setkey(UpVotes,"PostId")
  DownVotes <- votesdata[votesdata$VoteTypeId==3,.N,by= .(PostId)]
  setnames(DownVotes,c("PostId","N"),c("PostId","DownVotes"))
  setkey(DownVotes,"PostId")
  output <- UpVotes[DownVotes,on="PostId", DownVotes:= i.DownVotes]
  setnafill(output,fill=0)
  output
}

## ------------------------ Zadanie 6 ----------------------------

## ---- sqldf ----

df_sql_6 <- function(Votes){sqldf('
  SELECT PostId, UpVotes-DownVotes AS Votes FROM (
    SELECT UpVotesTab.PostId, UpVotesTab.UpVotes, IFNULL(DownVotesTab.DownVotes,0) AS DownVotes 
      FROM 
        (
          SELECT PostId, COUNT(*) AS UpVotes FROM Votes 
            WHERE VoteTypeId=2 GROUP BY PostId
        ) AS UpVotesTab
      LEFT JOIN 
        (
          SELECT PostId, COUNT(*) AS DownVotes 
            FROM Votes WHERE VoteTypeId=3 GROUP BY PostId
        ) AS DownVotesTab 
      ON UpVotesTab.PostId=DownVotesTab.PostId 
      UNION 
      SELECT DownVotesTab.PostId, IFNULL(UpVotesTab.UpVotes, 0) AS UpVotes, DownVotesTab.DownVotes 
        FROM 
          (
            SELECT PostId, COUNT(*) AS DownVotes FROM Votes
              WHERE VoteTypeId=3 GROUP BY PostId
          ) AS DownVotesTab 
        LEFT JOIN 
          (
            SELECT PostId, COUNT(*) AS UpVotes FROM Votes 
              WHERE VoteTypeId=2 GROUP BY PostId
          ) AS UpVotesTab 
        ON DownVotesTab.PostId=UpVotesTab.PostId
  )')}

## ---- bazowy ----

df_base_6 <- function(Votes){
  UpVotesTab <- Votes[Votes$VoteTypeId==2, "PostId", drop=FALSE]
  UpVotesTab <- aggregate(UpVotesTab$PostId, UpVotesTab["PostId"], length)
  colnames(UpVotesTab)[2] <- "UpVotes"
  DownVotesTab <- Votes[Votes$VoteTypeId==3, "PostId", drop=FALSE]
  DownVotesTab <- aggregate(DownVotesTab$PostId, DownVotesTab["PostId"], length)
  colnames(DownVotesTab)[2] <- "DownVotes"
  
  A <- merge(x = UpVotesTab, y = DownVotesTab,
             by ='PostId', all.x=TRUE, all.y = FALSE)
  A$DownVotes[is.na(A$DownVotes)] <- 0
  A$UpVotes[is.na(A$UpVotes)] <- 0
  A <- A[,c("PostId","UpVotes","DownVotes")]
  
  B <- merge(x = DownVotesTab, y = UpVotesTab,
             by ='PostId', all.x=TRUE, all.y = FALSE)
  B$UpVotes[is.na(B$UpVotes)] <- 0
  B <- B[,c("PostId","UpVotes","DownVotes")]
  x <- rbind(A, B)
  x$UpVotes <- x$UpVotes - x$DownVotes
  colnames(x)[2] <- "Votes"
  x[,c("PostId","Votes")]
}

## ---- dplyr ----

df_dplyr_6<-function(Votes){
  A <- df_dplyr_5(Votes)
  UpVotesTab <- filter(Votes,VoteTypeId==2)
  UpVotesTab <- group_by(UpVotesTab,PostId)
  UpVotesTab <- summarize(UpVotesTab,UpVotes=n())
  DownVotesTab <- filter(Votes,VoteTypeId==3)
  DownVotesTab <- group_by(DownVotesTab,PostId)
  DownVotesTab <- summarize(DownVotesTab,DownVotes=n())
  output <- left_join(DownVotesTab,UpVotesTab,by="PostId")
  output$DownVotes <- coalesce(output$DownVotes,0L)
  output$UpVotes <- coalesce(output$UpVotes,0L)
  B <- select(output, PostId, UpVotes, DownVotes)
  x <- union(A,B)
  x <- mutate(x,Votes=UpVotes-DownVotes)
  select(x,PostId,Votes)
}

## ---- data.table ----

df_table_6 <- function(Votes){
  votesdata <- as.data.table(Votes)
  UpVotes <- votesdata[VoteTypeId==2,]
  UpVotes <- UpVotes[,.(UpVotes=.N),by="PostId"]
  DownVotes <- votesdata[VoteTypeId==3,]
  DownVotes <- DownVotes[,.(DownVotes=.N),by="PostId"]
  x<-merge.data.table(UpVotes,DownVotes,by="PostId",all.x = TRUE)
  setnafill(x,fill=0)
  y<-merge.data.table(UpVotes,DownVotes,by="PostId",all.y = TRUE)
  setnafill(y,fill=0)
  output <- funion(x,y)
  output <- output[,.(PostId,Votes=output$UpVotes-output$DownVotes)]
  output <- setorder(output, PostId)
  output
}
