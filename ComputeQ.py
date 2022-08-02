import traceback


class ComputeQ:
    def __init__(self):
        self.remarksToIntervals = None
        self.problemQs = []

    def increasing(self, read1, read2):
        if read2 - read1 > 0:
            return True
        else:
            return False

    def getMax(self, reads):
        i = 0
        max = reads[0]
        maxIndex = 0
        for read in reads:
            if read > max:
                max = read
                maxIndex = i
            i = i + 1

        return max, maxIndex

    def getStartIndex(self, reads, maxIndex):
        stillDecreasing = True

        i = maxIndex
        while stillDecreasing and (i - 1) >= 0:
            difference = reads[i] - reads[i - 1]
            if difference < 0 or difference < 1:
                stillDecreasing = False
            i = i - 1

        return (i + 1)

    def getStopIndex(self, reads, maxIndex):
        stillDecreasing = True

        i = maxIndex
        while stillDecreasing and (i + 1) < len(reads):
            difference = reads[i] - reads[i + 1]
            if difference <= 0 or difference < 1:
                stillDecreasing = False
            i = i + 1

        return i

    def getAverage(self, readList):
        sum = 0
        for read in readList:
            sum = sum + read

        return (sum / float(len(readList)))

    def subtractBackground(self, base, humpReads):
        differences = []
        for read in humpReads:
            difference = read - base
            if difference > 0:
                differences.append(difference)
            else:
                differences.append(0)
        return differences

    def divideByTwo(self, minusBackground):
        corrected = []
        for read in minusBackground:
            corrected.append(float(read) / 2)
        return corrected

    def getAverages(self, correctedList, remarks):
        averages = []
        i = 0
        while i < len(correctedList) - 1:
            average = self.getAverage(correctedList[i:(i+2)])
            averages.append(average)
            i = i + 1
        return averages

    def multiplyBySeconds(self, averageList, remarks):
        multiplied = []
        i = 0
        for average in averageList:
            multiply = average * self.remarksToIntervals[remarks][i]
            multiplied.append(multiply)
            i = i + 1
        return multiplied

    def getSum(self, values):
        sum = 0
        for value in values:
            sum = sum + value
        return sum

    def computeQs(self, remarksToEc, remarksToIntervals, remarksToQList):
        self.remarksToEc = remarksToEc
        self.remarksToIntervals = remarksToIntervals
        self.remarksToQList = remarksToQList


        for remarks in self.remarksToEc.keys():
            print(remarks)
            print(self.remarksToEc[remarks])
            try:
                reads = self.remarksToEc[remarks]

                # get the bell curve:
                max, maxIndex = self.getMax(reads)
                startIndex = self.getStartIndex(reads, maxIndex)
                # stopIndex = self.getStopIndex(reads, maxIndex)

                # get the average before and after:
                background = self.getAverage(reads[:startIndex])
                minusBackground = self.subtractBackground(background, reads[startIndex:])
                corrected = self.divideByTwo(minusBackground)
                averages = self.getAverages(corrected, remarks)
                values = self.multiplyBySeconds(averages, remarks)
                moment = self.getSum(values)
                if len(remarks.split("-")) == 2:
                    print("grams: " + str(remarks.split("-")[-1]))
                    discharge = float(remarks.split("-")[-1]) / (moment / 1000)
                elif len(remarks.split(" ")) == 2:
                    print("grams: " + str(remarks.split(" ")[-1]))
                    discharge = float(remarks.split(" ")[-1]) / (moment / 1000)
                else:
                    print("grams: " + str(remarks[3:]))
                    discharge = float(remarks[3:]) / (moment / 1000)

                self.remarksToQList[remarks].append(discharge)

            except:
                print(traceback.format_exc())
                print("PROBLEM Q: " + remarks)
                self.problemQs.append(remarks)

        return self.remarksToEc, self.remarksToIntervals, self.remarksToQList