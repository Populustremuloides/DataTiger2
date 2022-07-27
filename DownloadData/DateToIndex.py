import matplotlib.pyplot as plt

from datetime import datetime
startYear = 18
dayToIndexRatio = 1 / (4 * 24)
indexToDayRatio = 4 * 24

def getDaysInYear(year):
    if year % 4 == 0:
        daysInYear = 366
    else:
        daysInYear = 365
    return daysInYear


def dateToIndex(year, month, day, startYear):
    # calculates the number of days since October 1, start year (2018)
    # will not work properly for any dates prior to that

    # year, month, day = date.split("-")
    year = int(year)
    month = int(month)
    day = int(day)

    index = -1

    if year % 4 == 0:  # if it is a leap year
        monthToDays = {1: 31, 2: 29, 3: 31, 4: 30, 5: 31, 6: 30, 7: 31, 8: 31, 9: 30, 10: 31, 11: 30, 12: 31}
    else:
        monthToDays = {1: 31, 2: 28, 3: 31, 4: 30, 5: 31, 6: 30, 7: 31, 8: 31, 9: 30, 10: 31, 11: 30, 12: 31}
    # print(index)
    monthVal = int(month)
    if startYear == year:
        # print(index)

        # then just calculate the day it is in the year and subract 304
        for preMonth in range(10, monthVal):  # add the previous months
            index += monthToDays[preMonth]

        index += day  # add the days
        # if index == None:
        #     print("start year")
        #     print(index)

        return index

    elif year > startYear:
        # add the residual from the first year
        for preMonth in range(10, 13):
            index += monthToDays[preMonth]

        # if index == None:
        #     print("pre anything")
        #     print(index)

        startYear += 1
        # now treat as if we were calculating days since Jan 1, (startYear + 1)

        # add the years *************************************************
        numYears = year - startYear
        currentYear = startYear
        for yearSinceStart in range(numYears):
            if currentYear % 4 == 0:
                daysInYear = 366
            else:
                daysInYear = 365
            currentYear += 1

            index = index + daysInYear

        # if index == None:
        #     print("year")
        #     print(index)

        # add the months ******************************************
        if year % 4 == 0:  # if it is a leap year
            monthToDays = {1: 31, 2: 29, 3: 31, 4: 30, 5: 31, 6: 30, 7: 31, 8: 31, 9: 30, 10: 31, 11: 30, 12: 31}
        else:
            monthToDays = {1: 31, 2: 28, 3: 31, 4: 30, 5: 31, 6: 30, 7: 31, 8: 31, 9: 30, 10: 31, 11: 30, 12: 31}

        monthVal = int(month)

        # if index == None:
        #     print("month")
        #     print(index)
        if monthVal > 1:
            for month in range(1, monthVal):  # don't include the current month because it isn't over yet!
                index = index + monthToDays[month]

        # if index == None:
        #     print("day")
        #     print(index)

        # add the days *******************************************
        index = index + day
        # if index == None:
        #     print(index)

        return index


def indexToDatetime(index, startYear):
    # index must represent days since Oct 1, startYear

    # put everything back into the framework of normal years (Jan 1, startYear)
    if (startYear % 4) == 0:  # if it is a leap year
        monthToDays = {1: 31, 2: 29, 3: 31, 4: 30, 5: 31, 6: 30, 7: 31, 8: 31, 9: 30, 10: 31, 11: 30, 12: 31}
    else:
        monthToDays = {1: 31, 2: 28, 3: 31, 4: 30, 5: 31, 6: 30, 7: 31, 8: 31, 9: 30, 10: 31, 11: 30, 12: 31}

    for month in range(1, 10):
        index += monthToDays[month]
    index += 1
    currentIndex = 0

    timeIndex = index - int(index)

    index = int(index)
    startYear = int(startYear)

    year = startYear
    while currentIndex < index:
        daysInYear = getDaysInYear(year)
        currentIndex += daysInYear
        year += 1

    year = year - 1
    currentIndex = currentIndex - daysInYear

    if getDaysInYear(year) == 366:  # if it is a leap year
        monthToDays = {1: 31, 2: 29, 3: 31, 4: 30, 5: 31, 6: 30, 7: 31, 8: 31, 9: 30, 10: 31, 11: 30, 12: 31}
    else:
        monthToDays = {1: 31, 2: 28, 3: 31, 4: 30, 5: 31, 6: 30, 7: 31, 8: 31, 9: 30, 10: 31, 11: 30, 12: 31}

    month = 1
    while currentIndex < index:
        currentIndex += monthToDays[month]
        month += 1

    month = month - 1
    currentIndex -= monthToDays[month]

    day = 1
    while currentIndex < index:
        currentIndex += 1
        day += 1
    day = day - 1

    hour = int(timeIndex * 24)
    timeIndex = timeIndex - (hour / 24)
    minute = int(timeIndex * 24 * 60)
    timeIndex = timeIndex - ((minute / 24) / 60)
    second = int(timeIndex * 24 * 60 * 60)

    # date = str(year) + "-" + str(month) + "-" + str(day)
    # time = str(hour) + ":" + str(minute) + ":" + str(second)

    return year, month, day, hour, minute, second

# def dateToIndex(year, month, day, index):
#     # calculates the number of days since Oct 1, startYear
#
#     # add years
#     difference = int(year) - startYear
#     for addition in range(1, difference):
#         if (2000 + startYear + addition) % 4 == 0:
#             index += 366
#         else:
#             index += 365
#     # add months
#     monthToDays = {"1": 31, "2": 28, "3": 31, "4": 30, "5": 31, "6": 30, "7": 31, "8": 31, "9": 30, "10": 31,
#                      "11": 30, "12": 31}
#     if int(year) % 4 == 0:
#         monthToDays = {"1": 31, "2": 29, "3": 31, "4": 30, "5": 31, "6": 30, "7": 31, "8": 31, "9": 30, "10": 31,
#                          "11": 30, "12": 31}
#
#     month_int = int(month)
#
#     for addition in range(1, month_int):
#         index += monthToDays[str(addition)]
#     # add days
#     index += int(day)
#
#     if startYear % 4 == 0:
#         monthToDays = {"1": 31, "2": 29, "3": 31, "4": 30, "5": 31, "6": 30, "7": 31, "8": 31, "9": 30, "10": 31,
#                          "11": 30, "12": 31}
#     else:
#         monthToDays = {"1": 31, "2": 28, "3": 31, "4": 30, "5": 31, "6": 30, "7": 31, "8": 31, "9": 30, "10": 31,
#                      "11": 30, "12": 31}
#     subtractionValue = 1
#     for i in range(1,10):
#         subtractionValue += monthToDays[str(i)]
#     index = index - subtractionValue
#
#     return index

def timeToIndex(hour, minute, second, index):
    # add hours
    index += int(hour) / 24
    # add minutes
    index += int(minute) / (24 * 60)
    # add seconds
    index += float(second) / (24 * 60 * 60)
    return index

def datetimeToIndex(year, month, day, hour, minute, second):
    index1 = dateToIndex(year, month, day, startYear)
    index = timeToIndex(hour, minute, second, index1)
    # if index > 647:
    #     print(year)
    #     print(month)
    #     print(day)
    #     print(hour)
    #     print(minute)
    #     print(second)
    return index

startIndex = datetimeToIndex(str(startYear), "10", "01", "00", "00", "00")
startIndex = round(startIndex / dayToIndexRatio) * dayToIndexRatio

def getDaysInYear(year):
    #sees if it's a leap year or not
    if year % 4 == 0:
        daysInYear = 366
    else:
        daysInYear = 365
    return daysInYear


# def indexToDatetime(index):
#     diff = index - startIndex
#     year = startYear
#     numDaysInYear = getDaysInYear(startYear)
#     while diff > numDaysInYear:
#         index = index - numDaysInYear
#         diff = index - startIndex
#         year += 1
#         numDaysInYear = getDaysInYear(year)
#
#     monthToDays = {0: 31, 1: 28, 2: 31, 3: 30, 4: 31, 5: 30, 6: 31, 7: 31, 8: 30, 9: 31,
#                      10: 30, 11: 31}
#     if int(year) % 4 == 0:
#         monthToDays = { 0:31, 1: 29, 2: 31, 3: 30, 4: 31, 5: 30, 6: 31, 7: 31, 8: 30, 9: 31,
#                          10: 30, 11: 31}
#     month = 9
#     numDaysInMonth = monthToDays[month]
#     while diff > numDaysInMonth:
#         index = index - numDaysInMonth
#         diff = index - startIndex
#
#         month = month + 1
#         month = month % 11
#
#         numDaysInMonth = monthToDays[month]
#
#     day = int(diff)
#     diff = diff - day
#
#     hour = int(diff * 24)
#     diff = diff - (hour / 24)
#     minute = int(diff * 24 * 60)
#     diff = diff - ((minute / 24) / 60)
#     second = int(diff * 24 * 60 * 60)
#
#     year = str(year)
#     month = str(month)
#     day = str(day)
#     hour = str(hour)
#     minute = str(minute)
#     second = str(second)
#
#     if len(year) == 1:
#         year = "0" + year
#     if len(month) == 1:
#         month = "0" + month
#     if len(day) == 1:
#         day = "0" + day
#     if len(hour) == 0:
#         hour = "0" + hour
#     if len(minute) == 0:
#         minute = "0" + minute
#     if len(second) == 0:
#         second = "0" + second
#     return year, month, day, hour, minute, second

oldIndices = []
newIndices = []
numOff = 0
for i in range(0,600):
    index = i
    for j in range(int(1 / dayToIndexRatio)):
        # index = index + dayToIndexRatio

        year, month, day, hour, minute, second = indexToDatetime(index, startYear)
        # print(index)
        date = str(year) + "-" + str(month) + "-" + str(day) + " " + str(hour) + ":" + str(minute) + ":" + str(second)
        # print(date)
        if month == "13":
            print(month)
        newIndex = datetimeToIndex(year, month, day, hour, minute, second)
        # print(newIndex)
        # input("")
        # newIndex += 365
        # newIndex = newIndex + startIndex

        newIndices.append(newIndex)
        oldIndices.append(index)
        if newIndex - index != 0:
            numOff += 1
        # print(newIndex - index)

    # get the number of months
# print(numOff)
# plt.plot(newIndices,label="new")
# plt.plot(oldIndices, label="old")
# plt.legend()
# plt.show()
# print(dateToIndex("18","10","01",18))