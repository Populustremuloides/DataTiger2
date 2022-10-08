---
# README DATATIGER
#### Author: Ethan McQuhae

---

## Welcome to Data Tiger!

Data Tiger is a data ingesting, storing, and processing infrastructure created for the Abbott Lab of Ecosystem Ecology. The Abbott Lab has spent the past 5 years collecting real-time data of water catchment chemistry in the Utah Lake watershed in response to a recent megafire in the watershed. Written in Python and SQL, Data Tiger has allowed the Abbott lab to make sense of the millions of lines of data they've collected over the past 5 years.

This README is made to help new users of Data Tiger to understand how it works and how they can become users of this system.

## Uploading

Uploading is carried out in the Data Tiger GUI, created in PyQt5. When uploading a new data set to Data Tiger, we have to determine the file type. In our lab we have used a plethora of sensors, each with their own formatting. The following code outlines determining this file type:

```{python}
def senseFileOrigin(self, path):
  file_type = self.sensor.senseFileOrigin(path)
  self.file_type = file_type
  return file_type
```

senseFileOrigin.py outlines this functions abilities. While wordy, it can be summed up with this simple example:

```{python}
if firstRow[0].endswith(".LOG"):
  return "field_eureka"
#if the file wished to be uploaded conforms with this criteria, it will return this specific file type, and so on with other files.
```

In order to ensure that we are always uploading clean data to our database we have installed various safe-guards that will then scan through the file and ensure that all of the data is clean. If the data is verified and deemed as clean, it is then uploaded to the database, handled in SQLite as follows:

```{python}
if self.databaseOpen:
  self.uploader.uploadFile(self.cursor, path, fileOrigin, self.allowDuplicates)
#databaseOpen will always = True if Data Tiger is able to open when first ran. 
```

When the file has been successfully uploaded, you will see a message in the GUI such as:

```{python}
Time to do a celebratory dance!
```

## Downloading

Downloading in Data Tiger has many facets. We have spent the majority of our time here in the back-end development. Below I will outline in depth each of the types of downloads that you can receive in Data Tiger. 

As you noticed, I did not spend a lot of time talking about the uploaders section, as there is a lot of automation in that section, no need to mess with it (and dont really want to).

#### Indicies
In order to keep data in order and to bridge gaps in logging for outside reasons(ie. sensor pulled, battery died, etc) there is an index created to keep everything in order

```{python}
dayToIndexRatio = 1 / (4 * 24)
# Each 15 minute interval is equal to ~0.01041 as an index
```

Whenever this line reads at the beginning of creating a data frame from the database, it is creating an index and assigning datetime values to each index number on a 15-minute interval from the day the project started (10/1/18) until the current datetime
```{python}
indexList = getIndexList()
dateList = getDateList(indexList)
```

#### DownloadTimeSeries














