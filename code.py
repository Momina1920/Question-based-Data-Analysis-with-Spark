from pyspark.sql import SparkSession
from pyspark import StorageLevel

if __name__ == "__main__":
    sk = SparkSession.builder.appName("Project").getOrCreate()

    df = sk.read.format("csv").option("header", "true").load("hdfs://master:9000/user/user6/project/data.csv")
    exceptions = sk.read.format("csv").option("header","true").load("hdfs://master:9000/user/user6/project/side.csv")
    
    #The only useful columns are "Country" and "Medal" so we select them
    #imp = df.rdd.map(lambda x: (x[10],x[8]))
    #We are only interested in gold medals so we filter the others out, we then create tuples ("Country",number of gold medals)
    #med = imp.filter(lambda x: x[0] == "Gold").map(lambda x: (x[1],1)).reduceByKey(lambda acc,x: x + acc)
    #Finally we sort the result in descending order
    #result0 = med.map(lambda x: (x[1],x[0])).sortByKey(0).collect()
    
    #Select the columns containing the country names and medals won
    imp = df.rdd.map(lambda x: (x[10],x[8]))
    #Create a different grouping for each type of medal for each country
    med = imp.map(lambda x: ((x[1],x[0]),1)).reduceByKey(lambda acc,x: x + acc)
    #Sort the result in alphabetical order
    result1 = med.sortByKey(1).persist(StorageLevel.MEMORY_ONLY).collect()
    result0 = med.sortByKey(1).filter(lambda x: x[0][1] == "Gold").map(lambda x: (x[1],x[0][0])).sortByKey(0).collect()
    
    #Select the columns containing discipline and gender of the event
    imp = df.rdd.map(lambda x: (x[3],x[9]))
    #In the "Event_gender" column an "X" marks mixed events
    med = imp.filter(lambda x: x[1] == "X").groupByKey()
    #Return the name of the disciplines with at least one mixed event
    result2 = med.map(lambda x: x[0]).collect()
    
    #Select the columns containing gender of the athlete, gender of the events and medals
    imp = df.rdd.map(lambda x: (x[6],x[9],x[10]))
    #Keep only mixed events and count the number of gold medals for the 2 genders
    med = imp.filter(lambda x: x[1] == "X" and x[2] == "Gold").map(lambda x: (x[0],1)).reduceByKey(lambda acc,x: x + acc)
    #Sort and format result
    result3 = med.map(lambda x: (x[1],x[0])).sortByKey(0).map(lambda x: (x[1],x[0])).collect()
    
    #Select disciplines and gender of events
    imp = df.rdd.map(lambda x: (x[3],x[9]))
    #Keep the disciplines with male events with no repetitions
    med = imp.filter(lambda x: x[1] == "M").map(lambda x: ((x[0]),x[1])).distinct()
    #Keep the disciplines with female events with no repetitions
    med2 = imp.filter(lambda x: x[1] == "W").map(lambda x: ((x[0]),x[1])).distinct()
    #Join the 2 results and keep only the disciplines that are either men only or women only
    med3 = med.fullOuterJoin(med2).filter(lambda x: x[1][0] is None or x[1][1] is None)
    #Format the result
    r1 = med3.filter(lambda x: x[1][1] is None).map(lambda x: ("Men Only",x[0])).reduceByKey(lambda acc,x: x + ", " + acc).collect()
    r2 = med3.filter(lambda x: x[1][0] is None).map(lambda x: ("Women Only",x[0])).reduceByKey(lambda acc,x: x + ", " + acc).collect()
    result4 = [r1,r2]
    
    #Select all the columns except name and gender of the athlete (the only 2 things that differentiate between teammates)
    imp = df.rdd.map(lambda x: (x[0],x[1],x[2],x[3],x[4],x[7],x[8],x[9],x[10]))
    #Sum the number of team members for each winning team
    med = imp.map(lambda x: ((x[0],x[1],x[2],x[3],x[4],x[5],x[6],x[7],x[8]),1)).reduceByKey(lambda acc,x: x + acc)
    #For each country sum the number of team members after removing the teams with only one member (single athlete sports)
    med2 = med.map(lambda x: (x[0][6],x[1])).filter(lambda x: x[1]>1).reduceByKey(lambda acc,x: x + acc).map(lambda x: (x[1],x[0]))
    #Sort the number of won medals and keep only the 3 best and worst results
    r1 = med2.sortByKey(0).take(3)
    r2 = med2.sortByKey(1).take(3)
    result5 = [r1,r2]
    
    #Select all the columns except name and gender of the athlete (the only 2 things that differentiate between teammates) 
    imp = df.rdd.map(lambda x: (x[0],x[1],x[2],x[3],x[4],x[7],x[8],x[9],x[10]))
    #Sum the number of team members for each winning team
    med = imp.map(lambda x: ((x[0],x[1],x[2],x[3],x[4],x[5],x[6],x[7],x[8]),1)).reduceByKey(lambda acc,x: x + acc)
    #For each country sum the number of team members after removing the teams with only one member (single athlete sports) 
    med2 = med.map(lambda x: (x[0][6],x[1])).filter(lambda x: x[1]>1).reduceByKey(lambda acc,x: x + acc)
    #Create the list of all the countries in the dataset
    ref = df.rdd.map(lambda x: (x[8],"")).distinct()
    #Return only the countries that won no medals for team sports
    result6 = ref.leftOuterJoin(med2).filter(lambda x: x[1][1] is None).map(lambda x: x[0]).collect()
    
    #Select the columns containing year of the olympics and country name
    imp = df.rdd.map(lambda x: (x[1],x[8]))
    #Sum all the medals that each country won for each olympics
    med = imp.map(lambda x: ((x[0],x[1]),1)).reduceByKey(lambda acc,x: x + acc).map(lambda x: ((x[0][1]),(x[0][0],x[1])))
    #Compare 2 different olympics and keep only the ones that are consecutives (they differ only by 4 years)
    med2 = med.join(med).filter(lambda x: int(x[1][1][0]) - int(x[1][0][0]) == 4)
    #Sort the differences in won medals and keep only the 3 best and worst results
    r1 = med2.map(lambda x: (x[1][1][1] - x[1][0][1],(x[1][1][0],x[0]))).sortByKey(0).take(3)
    r2 = med2.map(lambda x: (x[1][1][1] - x[1][0][1],(x[1][1][0],x[0]))).sortByKey(1).take(3)
    result7 = [r1,r2]
    
    #Select the columns containing disciplines and countries
    imp = df.rdd.map(lambda x: (x[3],x[8]))
    #Sum the number of medals for each country for each discipline
    med = imp.map(lambda x: ((x[0],x[1]),1)).reduceByKey(lambda acc,x: x + acc)
    #Keep only the countries that won the most medals for each discipline
    med2 = med.map(lambda x: (x[0][0],(x[0][1],x[1]))).reduceByKey(lambda acc,x: x if x[1] > acc[1] else acc)
    #Format the result
    result8 = med2.map(lambda x: (x[0],x[1][0],x[1][1])).collect()
    
    #First we need to create a table that contains to which country does the city where the olympics took place belong
    lookup = sk.sparkContext.parallelize([("Montreal","Canada"),("Moscow","Soviet Union"),("Los Angeles","United States"),("Seoul","Korea, South"),("Barcelona","Spain"),("Atlanta","United States"),("Sydney","Australia"),("Athens","Greece"),("Beijing","China")])
    #Select the columns containing the olympic city, country and medals
    imp = df.rdd.map(lambda x: (x[0],x[8],x[10]))
    #Keep only the gold medals and group them for each country for each city
    med = imp.filter(lambda x: x[2] == "Gold").map(lambda x: ((x[0],x[1]),1)).reduceByKey(lambda acc,x: x + acc)
    #Keep only the olympics where each country won the most gold medals
    med2 = med.map(lambda x: (x[0][1],(x[0][0],x[1]))).reduceByKey(lambda acc,x: x if x[1] > acc[1] else acc)
    #Check with the table we created if a country won the most gold medals when the city hosting the olympics belonged to that country
    med3 = med2.map(lambda x: (x[1][0],(x[0],x[1][1]))).join(lookup).filter(lambda x: x[1][0][0] == x[1][1])
    #Format the result
    result9 = med3.map(lambda x: (x[0],x[1][1],x[1][0][1])).collect()
    
    #Select name of the athlete and olympic year without repetitions (more than one medal won in that year)
    imp = df.rdd.map(lambda x: (x[5],x[1])).distinct()
    #Calculate in how many olympics did the athlete win a medal and keep only the ones that are greater than 3
    med = imp.map(lambda x: (x[0],1)).reduceByKey(lambda acc,x: x + acc).filter(lambda x: x[1] > 3)
    #Sort the result
    result10 = med.map(lambda x: (x[1],x[0])).sortByKey(0).collect()
    
    #Select name of the athlete, discipline and year of olympics without repetitions (more than one medal won in that year)
    imp = df.rdd.map(lambda x: ((x[5],x[3]),x[1])).distinct()
    #Keep only the athletes that won a medal more than 15 years after they won the first one
    med = imp.join(imp).filter(lambda x: int(x[1][1]) - int(x[1][0]) > 15)
    #To consider the whole career we need to only keep the biggest gap between two olympics
    med2 = med.map(lambda x: (x[0],(x[1],int(x[1][1]) - int(x[1][0])))).reduceByKey(lambda acc,x: x if x[1] > acc[1] else acc)
    #Sort and format the result
    result11 = med2.map(lambda x: (x[1][1],(x[1][0],x[0]))).sortByKey(0).map(lambda x: (x[1][1],x[1][0],x[0])).collect()
    
    #Select the colums containing the year, discipline, event, country, gender event and medals
    imp = df.rdd.map(lambda x: (x[1],x[3],x[4],x[8],x[9],x[10]))
    #Keep only the gold medal winners for non-mixed sports and remove duplicates (teammates in team sports are counted as 1)
    med = imp.filter(lambda x: x[5] == "Gold" and x[4] != "X").map(lambda x: ((x[0],x[1],x[2]),(x[3],x[4]))).distinct()
    #Keep only the sports in which the same country won both gold medals for the man and the women competition
    med2 = med.join(med).filter(lambda x: x[1][0][0] == x[1][1][0] and x[1][0][1] == "M" and x[1][1][1] == "W")
    #Format the result
    result12 = med2.map(lambda x: (x[0][0],x[0][1],x[0][2],x[1][0][0])).collect()
    
    #Select the columns containing the countries and the year of the olympics
    imp = df.rdd.map(lambda x: (x[8],x[1]))
    #Sum up the number of medals for each country for each year
    med = imp.map(lambda x: ((x[0],x[1]),1)).reduceByKey(lambda acc,x: x + acc)
    #Create the list of all the olympics in the dataset
    ref = df.rdd.map(lambda x: (x[1])).distinct()
    #Create the list of all the countries in the dataset, then cartesian product them together with the list of olympics
    ref2 = df.rdd.map(lambda x: (x[8])).distinct().cartesian(ref).map(lambda x: ((x[0],x[1]),""))
    #From the result remove the couple (country, year) if the country didn't participate that year
    ref3 = exceptions.rdd.map(lambda x: ((x[0],x[1]),"")).rightOuterJoin(ref2).filter(lambda x: x[1][0] is None).map(lambda x: (x[0],""))
    #If the country did participate but didn't have any medals for that year it should count as having "won" 0 medals
    med2 = ref3.leftOuterJoin(med).map(lambda x: (x[0][0],x[1][1]) if x[1][1] is not None else (x[0][0],0))
    #Calculate the average
    med3 = med2.aggregateByKey((0.,0.),lambda a,b: (a[0]+b, a[1]+1),lambda a,b: (a[0]+b[0],a[1]+b[1])).mapValues(lambda x: float(x[0]/x[1]))
    #Sort and format the result
    result13 = med3.map(lambda x: (x[1],x[0])).sortByKey(0).collect()
    
    #Select the columns containing the name of the athletes,country of origin and sport
    imp = df.rdd.map(lambda x:(x[5],x[8],x[2]))
    #Sum up the number of medals won for each tuple (athlete,country,sport)
    med = imp.map(lambda x:((x[0],x[1],x[2]),1)).reduceByKey(lambda acc,x:acc+x)
    #Return the athlete that won the most medals for each sport
    med2 = med.map(lambda x:(x[0][2],(x[0][0],x[0][1],x[1]))).reduceByKey(lambda a,b:a if a[2]>b[2] else b)
    #Format the result
    result14 = med2.map(lambda x: (x[0],(x[1][0],x[1][1]),x[1][2])).collect()
    
    #Select the columns containing the gender,country and sport gender
    imp = df.rdd.map(lambda x:(x[6],x[8],x[9]))
    #Keep only the mixed sports and sum up the number of medals for each country and gender
    med = imp.filter(lambda x:x[2]=="X").map(lambda x:((x[1],x[0]),1)).reduceByKey(lambda acc,x:acc+x)
    #Return the gender that performed better for each country
    med2 = med.map(lambda x:(x[0][0],(x[0][1],x[1]))).reduceByKey(lambda a,b:a if a[1]>b[1] else b)
    #Return the countries where women had better performance
    result15 = med2.filter(lambda x:(x[1][0]=="Women")).map(lambda x: (x[0],x[1][1])).collect()
    
    #Select the tuples (country and gender) and calculate the number of medals won for each
    imp = df.rdd.map(lambda x: ((x[8],x[6]),1)).reduceByKey(lambda acc,x:acc+x)
    #For each country create a result (gender, number of medals won by that gender, number of medals won by both genders)
    med = imp.map(lambda x:(x[0][0],(x[0][1],x[1]))).reduceByKey(lambda acc,x: (acc[0],acc[1],acc[1]+x[1]))
    #For each country return the tuple (percentage of female winners, percentage of male winners)
    med2 = med.map(lambda x: (x[0],(float(x[1][1])*100/x[1][-1],float(x[1][-1]-x[1][1])*100/x[1][-1])) if x[1][0] == "Man" else (x[0],((float(x[1][-1]-x[1][1])*100/x[1][-1]),float(x[1][1])*100/x[1][-1])))
    #Return the absolute value of the difference between the 2 percentages and the percentages aswell
    med3 = med2.map(lambda x: (abs(float(x[1][0]-x[1][1])),x[0],x[1]))
    #Sort the result and take the Top 3
    result16 = med3.sortByKey(1).take(3)
    
    #Select the columns containing the country, gender and medals
    imp = df.rdd.map(lambda x: (x[8],x[10],x[6]))
    #Sum up the gold medals won by female athletes
    med = imp.filter(lambda x:(x[2]=="Women" and x[1]=="Gold")).map(lambda x:(x[0],1)).reduceByKey(lambda acc,x:x+acc)
    #Sum up the gold medals won by male athletes
    med2 = imp.filter(lambda x:(x[2]=="Men" and x[1]=="Gold")).map(lambda x:(x[0],1)).reduceByKey(lambda acc,x:x+acc)
    #Return the country that won the most gold medals by female athletes
    r1 = med.map(lambda x:(x[1],x[0])).sortByKey(0).map(lambda x:("Women",x[0],x[1])).take(1)
    #Return the country that won the most gold medals by male athletes
    r2 = med2.map(lambda x:(x[1],x[0])).sortByKey(0).map(lambda x:("Man",x[0],x[1])).take(1)
    #Final result
    result17 = [r1,r2]
    
    #Select country, gender and medals
    imp = df.rdd.map(lambda x: (x[8],x[6],x[10]))
    #Select the countries in which at least one female athlete won a gold medal
    med = imp.filter(lambda x:(x[1]=="Women"and x[2]=="Gold")).map(lambda x: (x[0],""))
    #Keep a list of all the countries with no repetitions
    ref = df.rdd.map(lambda x: (x[8],"")).distinct()
    #Keep only the countries that belong to the list of countries but not the list of female gold medal winners
    med2 = ref.leftOuterJoin(med).filter(lambda x: x[1][1] is None)
    #Format the result
    result18 = med2.map(lambda x: x[0]).collect()
    
    #Select country, sport and event gender
    imp = df.rdd.map(lambda x: (x[8],x[2],x[9]))
    #Keep only Taekwondo for man and sum up the medals won by each country
    med = imp.filter(lambda x:(x[1]=="Taekwondo" and x[2]=="M")).map(lambda x:(x[0],1)).reduceByKey(lambda acc,x:acc+x)
    #Sort the result and take the Top 5
    result19 = med.map(lambda x:(x[1],x[0])).sortByKey(0).take(5)
    
    #Select year and country
    imp = df.rdd.map(lambda x: (x[1],x[8]))
    #Sum up the medals won by Canada for each year
    med = imp.filter(lambda x:(x[1]=="Canada")).map(lambda x:(x[0],1)).reduceByKey(lambda acc,x:acc+x)
    #Sort the result and take the best year
    result20 = med.map(lambda x:(x[1],x[0])).sortByKey(0).take(1)
    
    #Select country and discipline
    imp = df.rdd.map(lambda x: (x[8],x[3]))
    #Sum up the medals won by the United States for each discipline
    med =imp.filter(lambda x:(x[0]=="United States")).map(lambda x:(x[1],1)).reduceByKey(lambda acc,x:acc+x)
    #Sort the result and take the bast discipline
    result21 = med.map(lambda x:(x[1],x[0])).sortByKey(0).take(1)
    
	
    print("\nGold medals won, Country\n")
    for e in result0:
        print(e)
    
    print("\nCountry, Type of medal, Number of medals won\n")
    for e in result1:
        print(e)
        
    print("\nMixed disciplines\n")
    for e in result2:
       print(e)
    
    print("\nAthlete gender in mixed sports, Number of medals won\n")
    for e in result3:
       print(e)
    
    print("\nGender exclusive disciplines\n")
    for e in result4:
        print(e)
    
    print("\nCountries that won most and least medals in team sports\n")
    for e in result5:
        print(e)
    
    print("\nCountry that won no medals in team sports\n")
    for e in result6:
       print(e)
        
    print("\nBiggest increase and decrease in won medals between 2 consecutive olympics, Country, Second year\n")
    for e in result7:
        print(e)
    
    print("\nDiscipline, Country that won more medals in that discipline, Medals won\n")
    for e in result8:
        print(e)
        
    print("\nCity, Country that won the most gold medals the year they hosted the olympics in that city, Number of gold medals won\n")
    for e in result9:
        print(e)
    
    print("\nNumber of olympics where the athlete won a medal (more than 3), Athlete\n")
    for e in result10:
        print(e)
        
    print("\nAthlete, Discipline, Year of the first medal, Year of the last medal, Distance in time between the first and the last medal won (more than 15)\n")
    for e in result11:
        print(e)
        
    print("\nYear, Discipline, Event, Country that won in the same year both gold medals for the man and women competition\n")
    for e in result12:
        print(e)
        
    print("\nAverage number of medals won, Country\n")
    for (n,e) in result13:
        print("{0:.10f}".format(n),e)
        
    print("\nSport, Athlete that won the most medals, Country, Number of medals won\n")
    for e in result14:
        print(e)
        
    print("\nCountry in which women had a better performance than men in mixed sports, Number of medals won by women\n")
    for e in result15:
        print(e)
        
    print("\nDifference in percentage of female and male winners, Top 3 countries with the smallest difference, Female percentage of winners, Male percentage of winners\n")
    for (n,e,(f,m)) in result16:
        print("{0:.10f}".format(n),e,"({0:.10f},{1:.10f})".format(f,m))
        
    print("\nGender, Number of gold medals won, Country with the most gold medals won for that gender\n")
    for e in result17:
        print(e)
        
    print("\nCountries where a woman never won a gold medal\n")
    for e in result18:
       print(e)
    
    print("\nNumber of medals won in taekwondo men competition, Top 5 countries\n")
    for e in result19:
        print(e)
    
    print("\nNumber of gold medals won, Year in which Canada won the most gold medals\n")
    for e in result20:
        print(e)
      
    print("\nNumber of medals won, Discipline in which the United States won the most medals\n")
    for e in result21:
        print(e)
    
    sk.stop()