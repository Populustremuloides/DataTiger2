def getDaysInYear(year):
    if year % 4 == 0:
        daysInYear = 366
    else:
        daysInYear = 365
    return daysInYear


def usgs_date_to_index(date, startYear):

    year, month, day = date.split("-")
    year = int(year)
    month = int(month)
    day = int(day)

    index = 0 

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

    # add the months ******************************************
    if year % 4 == 0: # if it is a leap year
        monthToDays = {1:31, 2:29, 3:31, 4:30, 5:31, 6:30, 7:31, 8:31, 9:30, 10:31, 11:30, 12:31}
    else:
        monthToDays = {1:31, 2:28, 3:31, 4:30, 5:31, 6:30, 7:31, 8:31, 9:30, 10:31, 11:30, 12:31}

    monthVal = int(month)

    if monthVal > 1:
        for month in range(1, monthVal): # don't include the current month because it isn't over yet!
            index = index + monthToDays[month]

    # add the days *******************************************
    index = index + day

    return index



def indexToDate(index, startYear):
    
    currentIndex = 0

    index = int(index)
    startYear = int(startYear)
    
    year = startYear
    while currentIndex < index:
        daysInYear = getDaysInYear(year)
        currentIndex += daysInYear
        year += 1
    
    year = year - 1
    currentIndex = currentIndex - daysInYear

    if getDaysInYear(year) == 366: # if it is a leap year
        monthToDays = {1:31, 2:29, 3:31, 4:30, 5:31, 6:30, 7:31, 8:31, 9:30, 10:31, 11:30, 12:31}
    else:
        monthToDays = {1:31, 2:28, 3:31, 4:30, 5:31, 6:30, 7:31, 8:31, 9:30, 10:31, 11:30, 12:31}
    
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

    date = str(year) + "-" + str(month) + "-" + str(day)

    return date
