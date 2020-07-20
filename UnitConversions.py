import math
# FIXME: absolute vs specific conductivity - check with Ben

def identity(x):
    return x

def luxToLumens(x):
    # formula: c * lux * A^2 = lumens
    if x == "":
        return ""
    x = float(x)
    c = 0.09290304 # conversion constant
    A = 1
    Asqr = A * A
    return c * x * Asqr

def lumensToLux(x):
    # forumula: (10.76391 * x) / A^2
    if x == "":
        return ""
    x = float(x)
    c = 10.76391 # conversion constant
    A = 1
    Asqr = A * A
    return (c * x) / Asqr

def kpaTommHg(x):
    # formulat: kpa / .1333223684 = mmHg
    if x == "":
        return ""
    x = float(x)
    c = 0.1333223684 # conversion constant
    return (x / c)

def mmHgTokpa(x):
    if x == "":
        return ""
    x = float(x)
    c = 0.1333223684 # conversion constant
    return (x * c)

def mmHgToatm(x):
    if x == "":
        return ""
    x = float(x)
    c = 760 # conversion constant
    return (x / c)

def psiTommHg(x):
    if x == "":
        return ""
    x = float(x)
    c = 51.715 # conversion constant
    return x * c

def mmHgTopsi(x):
    if x == "":
        return ""
    x = float(x)
    c = 51.715 # conversion constant
    return x / c

def celciusToKelvin(x):
    if x == "":
        return ""
    x = float(x)
    c = 273.15 # conversion constant
    return x + c



def calculateCp(mmHgPres, tempC):
    # declare variables
    pAtm = mmHgToatm(mmHgPres)
    tempK = celciusToKelvin(tempC)
    pwv = math.exp(11.8571 - (3840.70 / tempK) - (216961 / math.pow(tempK, 2)))
    cStar = math.exp(7.7117 - (1.31403 * math.log(tempC + 45.93)))
    theta = 9.75e-4 - (1.426e-5 * tempC) + (6.436e-8 * math.pow(tempC, 2))

    # run the first equation
    numerator = ((1 - (pwv / pAtm)) * (1 - theta * pAtm))
    denominator = ((1 - pwv) * (1 - theta))
    cp = cStar * pAtm * (numerator / denominator)
    return cp

def doPercentTomgL(x, mmHgPres, tempC):
    if x == "":
        return ""
    cp = calculateCp(mmHgPres, tempC)
    domgL = (cp * x) / 100
    return domgL

def domgLToPercent(x, mmHgPres, tempC):
    if x == "":
        return ""
    cp = calculateCp(mmHgPres, tempC)
    percentDo = (100 * x) / cp
    return percentDo


def farenheitToCelcius(x):
    if x == "":
        return ""
    x = float(x)
    c1 = (5.0 / 9.0)
    c2 = 32
    return (x - c2) * c1

def celciusToFarenheit(x):
    if x == "":
        return ""
    x = float(x)
    c1 = (9.0 / 5.0)
    c2 = 32
    return ((x * c1) + c2)

