from CustomErrors import *

class ReadScanFP:
    def __init__(self, filePath):
        self.filePath = filePath
        cleanPath = self.filePath.replace("\\", "/")
        cleanPath = cleanPath.replace("//", "/")
        pathList = cleanPath.split("/")
        self.fileName = pathList[-1]

        self.error = -1
        self.noError = 0

        self.dateTimeIndex =  None
        self.statusIndex = None

        self.index200 = None
        self.index202 = None
        self.index205 = None
        self.index207 = None
        self.index210 = None
        self.index212 = None
        self.index215 = None
        self.index217 = None
        self.index220 = None
        self.index222 = None
        self.index225 = None
        self.index227 = None
        self.index230 = None
        self.index232 = None
        self.index235 = None
        self.index237 = None
        self.index240 = None
        self.index242 = None
        self.index245 = None
        self.index247 = None
        self.index250 = None
        self.index252 = None
        self.index255 = None
        self.index257 = None
        self.index260 = None
        self.index262 = None
        self.index265 = None
        self.index267 = None
        self.index270 = None
        self.index272 = None
        self.index275 = None
        self.index277 = None
        self.index280 = None
        self.index282 = None
        self.index285 = None
        self.index287 = None
        self.index290 = None
        self.index292 = None
        self.index295 = None
        self.index297 = None
        self.index300 = None
        self.index302 = None
        self.index305 = None
        self.index307 = None
        self.index310 = None
        self.index312 = None
        self.index315 = None
        self.index317 = None
        self.index320 = None
        self.index322 = None
        self.index325 = None
        self.index327 = None
        self.index330 = None
        self.index332 = None
        self.index335 = None
        self.index337 = None
        self.index340 = None
        self.index342 = None
        self.index345 = None
        self.index347 = None
        self.index350 = None
        self.index352 = None
        self.index355 = None
        self.index357 = None
        self.index360 = None
        self.index362 = None
        self.index365 = None
        self.index367 = None
        self.index370 = None
        self.index372 = None
        self.index375 = None
        self.index377 = None
        self.index380 = None
        self.index382 = None
        self.index385 = None
        self.index387 = None
        self.index390 = None
        self.index392 = None
        self.index395 = None
        self.index397 = None
        self.index400 = None
        self.index402 = None
        self.index405 = None
        self.index407 = None
        self.index410 = None
        self.index412 = None
        self.index415 = None
        self.index417 = None
        self.index420 = None
        self.index422 = None
        self.index425 = None
        self.index427 = None
        self.index430 = None
        self.index432 = None
        self.index435 = None
        self.index437 = None
        self.index440 = None
        self.index442 = None
        self.index445 = None
        self.index447 = None
        self.index450 = None
        self.index452 = None
        self.index455 = None
        self.index457 = None
        self.index460 = None
        self.index462 = None
        self.index465 = None
        self.index467 = None
        self.index470 = None
        self.index472 = None
        self.index475 = None
        self.index477 = None
        self.index480 = None
        self.index482 = None
        self.index485 = None
        self.index487 = None
        self.index490 = None
        self.index492 = None
        self.index495 = None
        self.index497 = None
        self.index500 = None
        self.index502 = None
        self.index505 = None
        self.index507 = None
        self.index510 = None
        self.index512 = None
        self.index515 = None
        self.index517 = None
        self.index520 = None
        self.index522 = None
        self.index525 = None
        self.index527 = None
        self.index530 = None
        self.index532 = None
        self.index535 = None
        self.index537 = None
        self.index540 = None
        self.index542 = None
        self.index545 = None
        self.index547 = None
        self.index550 = None
        self.index552 = None
        self.index555 = None
        self.index557 = None
        self.index560 = None
        self.index562 = None
        self.index565 = None
        self.index567 = None
        self.index570 = None
        self.index572 = None
        self.index575 = None
        self.index577 = None
        self.index580 = None
        self.index582 = None
        self.index585 = None
        self.index587 = None
        self.index590 = None
        self.index592 = None
        self.index595 = None
        self.index597 = None
        self.index600 = None
        self.index602 = None
        self.index605 = None
        self.index607 = None
        self.index610 = None
        self.index612 = None
        self.index615 = None
        self.index617 = None
        self.index620 = None
        self.index622 = None
        self.index625 = None
        self.index627 = None
        self.index630 = None
        self.index632 = None
        self.index635 = None
        self.index637 = None
        self.index640 = None
        self.index642 = None
        self.index645 = None
        self.index647 = None
        self.index650 = None
        self.index652 = None
        self.index655 = None
        self.index657 = None
        self.index660 = None
        self.index662 = None
        self.index665 = None
        self.index667 = None
        self.index670 = None
        self.index672 = None
        self.index675 = None
        self.index677 = None
        self.index680 = None
        self.index682 = None
        self.index685 = None
        self.index687 = None
        self.index690 = None
        self.index692 = None
        self.index695 = None
        self.index697 = None
        self.index700 = None
        self.index702 = None
        self.index705 = None
        self.index707 = None
        self.index710 = None
        self.index712 = None
        self.index715 = None
        self.index717 = None
        self.index720 = None
        self.index722 = None
        self.index725 = None
        self.index727 = None
        self.index730 = None
        self.index732 = None
        self.index735 = None
        self.index737 = None
        self.index740 = None
        self.index742 = None
        self.index745 = None
        self.index747 = None
        self.index750 = None


        self.dateTimeValue =  None
        self.statusValue = None

        self.value200 = None
        self.value202 = None
        self.value205 = None
        self.value207 = None
        self.value210 = None
        self.value212 = None
        self.value215 = None
        self.value217 = None
        self.value220 = None
        self.value222 = None
        self.value225 = None
        self.value227 = None
        self.value230 = None
        self.value232 = None
        self.value235 = None
        self.value237 = None
        self.value240 = None
        self.value242 = None
        self.value245 = None
        self.value247 = None
        self.value250 = None
        self.value252 = None
        self.value255 = None
        self.value257 = None
        self.value260 = None
        self.value262 = None
        self.value265 = None
        self.value267 = None
        self.value270 = None
        self.value272 = None
        self.value275 = None
        self.value277 = None
        self.value280 = None
        self.value282 = None
        self.value285 = None
        self.value287 = None
        self.value290 = None
        self.value292 = None
        self.value295 = None
        self.value297 = None
        self.value300 = None
        self.value302 = None
        self.value305 = None
        self.value307 = None
        self.value310 = None
        self.value312 = None
        self.value315 = None
        self.value317 = None
        self.value320 = None
        self.value322 = None
        self.value325 = None
        self.value327 = None
        self.value330 = None
        self.value332 = None
        self.value335 = None
        self.value337 = None
        self.value340 = None
        self.value342 = None
        self.value345 = None
        self.value347 = None
        self.value350 = None
        self.value352 = None
        self.value355 = None
        self.value357 = None
        self.value360 = None
        self.value362 = None
        self.value365 = None
        self.value367 = None
        self.value370 = None
        self.value372 = None
        self.value375 = None
        self.value377 = None
        self.value380 = None
        self.value382 = None
        self.value385 = None
        self.value387 = None
        self.value390 = None
        self.value392 = None
        self.value395 = None
        self.value397 = None
        self.value400 = None
        self.value402 = None
        self.value405 = None
        self.value407 = None
        self.value410 = None
        self.value412 = None
        self.value415 = None
        self.value417 = None
        self.value420 = None
        self.value422 = None
        self.value425 = None
        self.value427 = None
        self.value430 = None
        self.value432 = None
        self.value435 = None
        self.value437 = None
        self.value440 = None
        self.value442 = None
        self.value445 = None
        self.value447 = None
        self.value450 = None
        self.value457 = None
        self.value460 = None
        self.value462 = None
        self.value465 = None
        self.value467 = None
        self.value470 = None
        self.value472 = None
        self.value475 = None
        self.value477 = None
        self.value480 = None
        self.value482 = None
        self.value485 = None
        self.value487 = None
        self.value490 = None
        self.value492 = None
        self.value495 = None
        self.value497 = None
        self.value500 = None
        self.value502 = None
        self.value505 = None
        self.value507 = None
        self.value510 = None
        self.value512 = None
        self.value515 = None
        self.value517 = None
        self.value520 = None
        self.value522 = None
        self.value525 = None
        self.value527 = None
        self.value530 = None
        self.value532 = None
        self.value535 = None
        self.value537 = None
        self.value540 = None
        self.value542 = None
        self.value545 = None
        self.value547 = None
        self.value550 = None
        self.value552 = None
        self.value555 = None
        self.value557 = None
        self.value560 = None
        self.value562 = None
        self.value565 = None
        self.value567 = None
        self.value570 = None
        self.value572 = None
        self.value575 = None
        self.value577 = None
        self.value580 = None
        self.value582 = None
        self.value585 = None
        self.value587 = None
        self.value590 = None
        self.value592 = None
        self.value595 = None
        self.value597 = None
        self.value600 = None
        self.value602 = None
        self.value605 = None
        self.value607 = None
        self.value610 = None
        self.value612 = None
        self.value615 = None
        self.value617 = None
        self.value620 = None
        self.value622 = None
        self.value625 = None
        self.value627 = None
        self.value630 = None
        self.value632 = None
        self.value635 = None
        self.value637 = None
        self.value640 = None
        self.value642 = None
        self.value645 = None
        self.value647 = None
        self.value650 = None
        self.value652 = None
        self.value655 = None
        self.value657 = None
        self.value660 = None
        self.value662 = None
        self.value665 = None
        self.value667 = None
        self.value670 = None
        self.value672 = None
        self.value675 = None
        self.value677 = None
        self.value680 = None
        self.value682 = None
        self.value685 = None
        self.value687 = None
        self.value690 = None
        self.value692 = None
        self.value695 = None
        self.value697 = None
        self.value700 = None
        self.value702 = None
        self.value705 = None
        self.value707 = None
        self.value710 = None
        self.value712 = None
        self.value715 = None
        self.value717 = None
        self.value720 = None
        self.value722 = None
        self.value725 = None
        self.value727 = None
        self.value730 = None
        self.value732 = None
        self.value735 = None
        self.value737 = None
        self.value740 = None
        self.value742 = None
        self.value745 = None
        self.value747 = None
        self.value750 = None


    def resetValues(self):
        self.value200 = None
        self.value202 = None
        self.value205 = None
        self.value207 = None
        self.value210 = None
        self.value212 = None
        self.value215 = None
        self.value217 = None
        self.value220 = None
        self.value222 = None
        self.value225 = None
        self.value227 = None
        self.value230 = None
        self.value232 = None
        self.value235 = None
        self.value237 = None
        self.value240 = None
        self.value242 = None
        self.value245 = None
        self.value247 = None
        self.value250 = None
        self.value252 = None
        self.value255 = None
        self.value257 = None
        self.value260 = None
        self.value262 = None
        self.value265 = None
        self.value267 = None
        self.value270 = None
        self.value272 = None
        self.value275 = None
        self.value277 = None
        self.value280 = None
        self.value282 = None
        self.value285 = None
        self.value287 = None
        self.value290 = None
        self.value292 = None
        self.value295 = None
        self.value297 = None
        self.value300 = None
        self.value302 = None
        self.value305 = None
        self.value307 = None
        self.value310 = None
        self.value312 = None
        self.value315 = None
        self.value317 = None
        self.value320 = None
        self.value322 = None
        self.value325 = None
        self.value327 = None
        self.value330 = None
        self.value332 = None
        self.value335 = None
        self.value337 = None
        self.value340 = None
        self.value342 = None
        self.value345 = None
        self.value347 = None
        self.value350 = None
        self.value352 = None
        self.value355 = None
        self.value357 = None
        self.value360 = None
        self.value362 = None
        self.value365 = None
        self.value367 = None
        self.value370 = None
        self.value372 = None
        self.value375 = None
        self.value377 = None
        self.value380 = None
        self.value382 = None
        self.value385 = None
        self.value387 = None
        self.value390 = None
        self.value392 = None
        self.value395 = None
        self.value397 = None
        self.value400 = None
        self.value402 = None
        self.value405 = None
        self.value407 = None
        self.value410 = None
        self.value412 = None
        self.value415 = None
        self.value417 = None
        self.value420 = None
        self.value422 = None
        self.value425 = None
        self.value427 = None
        self.value430 = None
        self.value432 = None
        self.value435 = None
        self.value437 = None
        self.value440 = None
        self.value442 = None
        self.value445 = None
        self.value447 = None
        self.value450 = None
        self.value457 = None
        self.value460 = None
        self.value462 = None
        self.value465 = None
        self.value467 = None
        self.value470 = None
        self.value472 = None
        self.value475 = None
        self.value477 = None
        self.value480 = None
        self.value482 = None
        self.value485 = None
        self.value487 = None
        self.value490 = None
        self.value492 = None
        self.value495 = None
        self.value497 = None
        self.value500 = None
        self.value502 = None
        self.value505 = None
        self.value507 = None
        self.value510 = None
        self.value512 = None
        self.value515 = None
        self.value517 = None
        self.value520 = None
        self.value522 = None
        self.value525 = None
        self.value527 = None
        self.value530 = None
        self.value532 = None
        self.value535 = None
        self.value537 = None
        self.value540 = None
        self.value542 = None
        self.value545 = None
        self.value547 = None
        self.value550 = None
        self.value552 = None
        self.value555 = None
        self.value557 = None
        self.value560 = None
        self.value562 = None
        self.value565 = None
        self.value567 = None
        self.value570 = None
        self.value572 = None
        self.value575 = None
        self.value577 = None
        self.value580 = None
        self.value582 = None
        self.value585 = None
        self.value587 = None
        self.value590 = None
        self.value592 = None
        self.value595 = None
        self.value597 = None
        self.value600 = None
        self.value602 = None
        self.value605 = None
        self.value607 = None
        self.value610 = None
        self.value612 = None
        self.value615 = None
        self.value617 = None
        self.value620 = None
        self.value622 = None
        self.value625 = None
        self.value627 = None
        self.value630 = None
        self.value632 = None
        self.value635 = None
        self.value637 = None
        self.value640 = None
        self.value642 = None
        self.value645 = None
        self.value647 = None
        self.value650 = None
        self.value652 = None
        self.value655 = None
        self.value657 = None
        self.value660 = None
        self.value662 = None
        self.value665 = None
        self.value667 = None
        self.value670 = None
        self.value672 = None
        self.value675 = None
        self.value677 = None
        self.value680 = None
        self.value682 = None
        self.value685 = None
        self.value687 = None
        self.value690 = None
        self.value692 = None
        self.value695 = None
        self.value697 = None
        self.value700 = None
        self.value702 = None
        self.value705 = None
        self.value707 = None
        self.value710 = None
        self.value712 = None
        self.value715 = None
        self.value717 = None
        self.value720 = None
        self.value722 = None
        self.value725 = None
        self.value727 = None
        self.value730 = None
        self.value732 = None
        self.value735 = None
        self.value737 = None
        self.value740 = None
        self.value742 = None
        self.value745 = None
        self.value747 = None
        self.value750 = None





    def readBatch(self, header, columns):

        self.spectrolyzer = header[0].split("_")[0]
        newSpec = ""
        for c in self.spectrolyzer:
            if c.isnumeric():
                newSpec = newSpec + c
        self.spectrolyzer = newSpec

        self.assignColumnIndices(columns)

    def assignColumnIndices(self, columns):

        i = 0
        for column in columns:
            column = str(column).lower()

            if "date" in column:
                self.dateTimeIndex = i
            elif "measurement interval" in column:
                self.dateTimeIndex = i
            elif "status" in column:
                self.statusIndex = i
            elif "200" in column:
                self.index200 = i
            elif "202" in column:
                self.index202 = i
            elif "205" in column:
                self.index205 = i
            elif "207" in column:
                self.index207 = i
            elif "210" in column:
                self.index210 = i
            elif "212" in column:
                self.index212 = i
            elif "215" in column:
                self.index215 = i
            elif "2170" in column:
                self.index217 = i
            elif "220" in column:
                self.index220 = i
            elif "222" in column:
                self.index222 = i
            elif "225" in column:
                self.index225 = i
            elif "227" in column:
                self.index227 = i
            elif "230" in column:
                self.index230 = i
            elif "232" in column:
                self.index232 = i
            elif "235" in column:
                self.index235 = i
            elif "237" in column:
                self.index237 = i
            elif "240" in column:
                self.index240 = i
            elif "242" in column:
                self.index242 = i
            elif "245" in column:
                self.index245 = i
            elif "247" in column:
                self.index247 = i
            elif "250" in column:
                self.index250 = i
            elif "252" in column:
                self.index252 = i
            elif "255" in column:
                self.index255 = i
            elif "257" in column:
                self.index257 = i
            elif "260" in column:
                self.index260 = i
            elif "262" in column:
                self.index262 = i
            elif "265" in column:
                self.index265 = i
            elif "267" in column:
                self.index267 = i
            elif "270" in column:
                self.index270 = i
            elif "272" in column:
                self.index272 = i
            elif "275" in column:
                self.index275 = i
            elif "277" in column:
                self.index277 = i
            elif "280" in column:
                self.index280 = i
            elif "282" in column:
                self.index282 = i
            elif "285" in column:
                self.index285 = i
            elif "287"in column:
                self.index287 = i
            elif "290" in column:
                self.index290 = i
            elif "292" in column:
                self.index292 = i
            elif "295" in column:
                self.index295 = i
            elif "297"in column:
                self.index297 = i
            elif "300" in column:
                self.index300 = i
            elif "302" in column:
                self.index302 = i
            elif "305" in column:
                self.index305 = i
            elif "307" in column:
                self.index307 = i
            elif "310" in column:
                self.index310 = i
            elif "312" in column:
                self.index312 = i
            elif "315" in column:
                self.index315 = i
            elif "317" in column:
                self.index317 = i
            elif "320" in column:
                self.index320 = i
            elif "322" in column:
                self.index322 = i
            elif "325" in column:
                self.index325 = i
            elif "327" in column:
                self.index327 = i
            elif "330" in column:
                self.index330 = i
            elif "332" in column:
                self.index332 = i
            elif "335" in column:
                self.index335 = i
            elif "337" in column:
                self.index337 = i
            elif "340" in column:
                self.index340 = i
            elif "342" in column:
                self.index342 = i
            elif "345" in column:
                self.index345 = i
            elif "347" in column:
                self.index347 = i
            elif "350" in column:
                self.index350 = i
            elif "352" in column:
                self.index352 = i
            elif "355" in column:
                self.index355 = i
            elif "357" in column:
                self.index357 = i
            elif "360" in column:
                self.index360 = i
            elif "362" in column:
                self.index362 = i
            elif "365" in column:
                self.index365 = i
            elif "367" in column:
                self.index367 = i
            elif "370" in column:
                self.index370 = i
            elif "372" in column:
                self.index372 = i
            elif "375" in column:
                self.index375 = i
            elif "377" in column:
                self.index377 = i
            elif "380" in column:
                self.index380 = i
            elif "382" in column:
                self.index382 = i
            elif "385" in column:
                self.index385 = i
            elif "387" in column:
                self.index387 = i
            elif "390" in column:
                self.index390 = i
            elif "392" in column:
                self.index392 = i
            elif "395" in column:
                self.index395 = i
            elif "397" in column:
                self.index397 = i
            elif "400" in column:
                self.index400 = i
            elif "402" in column:
                self.index402 = i
            elif "405" in column:
                self.index405 = i
            elif "407" in column:
                self.index407 = i
            elif "410" in column:
                self.index410 = i
            elif "412" in column:
                self.index412 = i
            elif "415" in column:
                self.index415 = i
            elif "417" in column:
                self.index417 = i
            elif "420" in column:
                self.index420 = i
            elif "422" in column:
                self.index422 = i
            elif "425" in column:
                self.index425 = i
            elif "427" in column:
                self.index427 = i
            elif "430" in column:
                self.index430 = i
            elif "432" in column:
                self.index432 = i
            elif "435" in column:
                self.index435 = i
            elif "437" in column:
                self.index437 = i
            elif "440" in column:
                self.index440 = i
            elif "442" in column:
                self.index442 = i
            elif "445" in column:
                self.index445 = i
            elif "447" in column:
                self.index447 = i
            elif "450" in column:
                self.index450 = i
            elif "452" in column:
                self.index452 = i
            elif "455" in column:
                self.index455 = i
            elif "457" in column:
                self.index457 = i
            elif "460" in column:
                self.index460 = i
            elif "462" in column:
                self.index462 = i
            elif "465" in column:
                self.index465 = i
            elif "467" in column:
                self.index467 = i
            elif "470" in column:
                self.index470 = i
            elif "472" in column:
                self.index472 = i
            elif "475" in column:
                self.index475 = i
            elif "477" in column:
                self.index477 = i
            elif "480" in column:
                self.index480 = i
            elif "482" in column:
                self.index482 = i
            elif "485" in column:
                self.index485 = i
            elif "487" in column:
                self.index487 = i
            elif "490" in column:
                self.index490 = i
            elif "492" in column:
                self.index492 = i
            elif "495" in column:
                self.index495 = i
            elif "497" in column:
                self.index497 = i
            elif "500" in column:
                self.index500 = i
            elif "502" in column:
                self.index502 = i
            elif "505" in column:
                self.index505 = i
            elif "507" in column:
                self.index507 = i
            elif "510" in column:
                self.index510 = i
            elif "512" in column:
                self.index512 = i
            elif "515" in column:
                self.index515 = i
            elif "517" in column:
                self.index517 = i
            elif "520" in column:
                self.index520 = i
            elif "522" in column:
                self.index522 = i
            elif "525" in column:
                self.index525 = i
            elif "527" in column:
                self.index527 = i
            elif "530" in column:
                self.index530 = i
            elif "532" in column:
                self.index532 = i
            elif "535" in column:
                self.index535 = i
            elif "537" in column:
                self.index537 = i
            elif "540" in column:
                self.index540 = i
            elif "542" in column:
                self.index542 = i
            elif "545" in column:
                self.index545 = i
            elif "547" in column:
                self.index547 = i
            elif "550" in column:
                self.index550 = i
            elif "552" in column:
                self.index552 = i
            elif "555" in column:
                self.index555 = i
            elif "557" in column:
                self.index557 = i
            elif "560" in column:
                self.index560 = i
            elif "562" in column:
                self.index562 = i
            elif "565" in column:
                self.index565 = i
            elif "567" in column:
                self.index567 = i
            elif "570" in column:
                self.index570 = i
            elif "572" in column:
                self.index572 = i
            elif "575" in column:
                self.index575 = i
            elif "577" in column:
                self.index577 = i
            elif "580" in column:
                self.index580 = i
            elif "582" in column:
                self.index582 = i
            elif "585" in column:
                self.index585 = i
            elif "587" in column:
                self.index587 = i
            elif "590" in column:
                self.index590 = i
            elif "592" in column:
                self.index592 = i
            elif "595" in column:
                self.index595 = i
            elif "597" in column:
                self.index597 = i
            elif "600" in column:
                self.index600 = i
            elif "602" in column:
                self.index602 = i
            elif "605" in column:
                self.index605 = i
            elif "607" in column:
                self.index607 = i
            elif "610" in column:
                self.index610 = i
            elif "612" in column:
                self.index612 = i
            elif "615" in column:
                self.index615 = i
            elif "617" in column:
                self.index617 = i
            elif "620" in column:
                self.index620 = i
            elif "622" in column:
                self.index622 = i
            elif "625" in column:
                self.index625 = i
            elif "627" in column:
                self.index627 = i
            elif "630" in column:
                self.index630 = i
            elif "632" in column:
                self.index632 = i
            elif "635" in column:
                self.index635 = i
            elif "637" in column:
                self.index637 = i
            elif "640" in column:
                self.index640 = i
            elif "642" in column:
                self.index642 = i
            elif "645" in column:
                self.index645 = i
            elif "647" in column:
                self.index647 = i
            elif "650" in column:
                self.index650 = i
            elif "652" in column:
                self.index652 = i
            elif "655" in column:
                self.index655 = i
            elif "657" in column:
                self.index657 = i
            elif "660" in column:
                self.index660 = i
            elif "662" in column:
                self.index662 = i
            elif "665" in column:
                self.index665 = i
            elif "667" in column:
                self.index667 = i
            elif "670" in column:
                self.index670 = i
            elif "672" in column:
                self.index672 = i
            elif "675" in column:
                self.index675 = i
            elif "677" in column:
                self.index677 = i
            elif "680" in column:
                self.index680 = i
            elif "682" in column:
                self.index682 = i
            elif "685" in column:
                self.index685 = i
            elif "687" in column:
                self.index687 = i
            elif "690" in column:
                self.index690 = i
            elif "692" in column:
                self.index692 = i
            elif "695" in column:
                self.index695 = i
            elif "697" in column:
                self.index697 = i
            elif "700" in column:
                self.index700 = i
            elif "702" in column:
                self.index702 = i
            elif "705" in column:
                self.index705 = i
            elif "707" in column:
                self.index707 = i
            elif "710" in column:
                self.index710 = i
            elif "712" in column:
                self.index712 = i
            elif "715" in column:
                self.index715 = i
            elif "717" in column:
                self.index717 = i
            elif "720" in column:
                self.index720 = i
            elif "722" in column:
                self.index722 = i
            elif "725" in column:
                self.index725 = i
            elif "727" in column:
                self.index727 = i
            elif "730" in column:
                self.index730 = i
            elif "732" in column:
                self.index732 = i
            elif "735" in column:
                self.index735 = i
            elif "737" in column:
                self.index737 = i
            elif "740" in column:
                self.index740 = i
            elif "742" in column:
                self.index742 = i
            elif "745" in column:
                self.index745 = i
            elif "747" in column:
                self.index747 = i
            elif "750" in column:
                self.index750 = i

            i = i + 1

    def readRow(self, row):

        self.resetValues()
        if self.dateTimeIndex == None:
            return self.error
        self.dateTimeValue = row[self.dateTimeIndex]
        self.dateTimeValue = self.dateTimeValue.replace(".","-")

        if self.index200 != None:
            self.value200 = row[self.index200]
        if self.index202 != None:
            self.value202 = row[self.index202]
        if self.index205 != None:
            self.value205 = row[self.index205]
        if self.index207 != None:
            self.value207 = row[self.index207]
        if self.index210 != None:
            self.value210 = row[self.index210]
        if self.index212 != None:
            self.value212 = row[self.index212]
        if self.index215 != None:
            self.value215 = row[self.index215]
        if self.index217 != None:
            self.value217 = row[self.index217]
        if self.index220 != None:
            self.value220 = row[self.index220]
        if self.index222 != None:
            self.value222 = row[self.index222]
        if self.index225 != None:
            self.value225 = row[self.index225]
        if self.index227 != None:
            self.value227 = row[self.index227]
        if self.index230 != None:
            self.value230 = row[self.index230]
        if self.index232 != None:
            self.value232 = row[self.index232]
        if self.index235 != None:
            self.value235 = row[self.index235]
        if self.index237 != None:
            self.valu2237 = row[self.index237]
        if self.index240 != None:
            self.value240 = row[self.index240]
        if self.index242 != None:
            self.value242 = row[self.index242]
        if self.index245 != None:
            self.value245 = row[self.index245]
        if self.index247 != None:
            self.value247 = row[self.index247]
        if self.index250 != None:
            self.value250 = row[self.index250]
        if self.index252 != None:
            self.value252 = row[self.index252]
        if self.index255 != None:
            self.value255 = row[self.index255]
        if self.index257 != None:
            self.value257 = row[self.index257]
        if self.index260 != None:
            self.value260 = row[self.index260]
        if self.index262 != None:
            self.value262 = row[self.index262]
        if self.index265 != None:
            self.value265 = row[self.index265]
        if self.index267 != None:
            self.value267 = row[self.index267]
        if self.index270 != None:
            self.value270 = row[self.index270]
        if self.index272 != None:
            self.value272 = row[self.index272]
        if self.index275 != None:
            self.value275 = row[self.index275]
        if self.index277 != None:
            self.value277 = row[self.index277]
        if self.index280 != None:
            self.value280 = row[self.index280]
        if self.index282 != None:
            self.value282 = row[self.index282]
        if self.index285 != None:
            self.value285 = row[self.index285]
        if self.index287 != None:
            self.value287 = row[self.index287]
        if self.index290 != None:
            self.value290 = row[self.index290]
        if self.index292 != None:
            self.value292 = row[self.index292]
        if self.index295 != None:
            self.value295 = row[self.index295]
        if self.index297 != None:
            self.value297 = row[self.index297]
        if self.index300 != None:
            self.value300 = row[self.index300]
        if self.index302 != None:
            self.value302 = row[self.index302]
        if self.index305 != None:
            self.value305 = row[self.index305]
        if self.index307 != None:
            self.value307 = row[self.index307]
        if self.index310 != None:
            self.value310 = row[self.index310]
        if self.index312 != None:
            self.value312 = row[self.index312]
        if self.index315 != None:
            self.value315 = row[self.index315]
        if self.index317 != None:
            self.value317 = row[self.index317]
        if self.index320 != None:
            self.value320 = row[self.index320]
        if self.index322 != None:
            self.value322 = row[self.index322]
        if self.index325 != None:
            self.value325 = row[self.index325]
        if self.index327 != None:
            self.value327 = row[self.index327]
        if self.index330 != None:
            self.value330 = row[self.index330]
        if self.index332 != None:
            self.value332 = row[self.index332]
        if self.index335 != None:
            self.value335 = row[self.index335]
        if self.index337 != None:
            self.value337 = row[self.index337]
        if self.index340 != None:
            self.value340 = row[self.index340]
        if self.index342 != None:
            self.value342 = row[self.index342]
        if self.index345 != None:
            self.value345 = row[self.index345]
        if self.index347 != None:
            self.value347 = row[self.index347]
        if self.index350 != None:
            self.value350 = row[self.index350]
        if self.index352 != None:
            self.value352 = row[self.index352]
        if self.index355 != None:
            self.value355 = row[self.index355]
        if self.index357 != None:
            self.value357 = row[self.index357]
        if self.index360 != None:
            self.value360 = row[self.index360]
        if self.index362 != None:
            self.value362 = row[self.index362]
        if self.index365 != None:
            self.value365 = row[self.index365]
        if self.index367 != None:
            self.value367 = row[self.index367]
        if self.index370 != None:
            self.value370 = row[self.index370]
        if self.index372 != None:
            self.value372 = row[self.index372]
        if self.index375 != None:
            self.value375 = row[self.index375]
        if self.index377 != None:
            self.value377 = row[self.index377]
        if self.index380 != None:
            self.value380 = row[self.index380]
        if self.index382 != None:
            self.value382 = row[self.index382]
        if self.index385 != None:
            self.value385 = row[self.index385]
        if self.index387 != None:
            self.value387 = row[self.index387]
        if self.index390 != None:
            self.value390 = row[self.index390]
        if self.index392 != None:
            self.value392 = row[self.index392]
        if self.index395 != None:
            self.value395 = row[self.index395]
        if self.index397 != None:
            self.value397 = row[self.index397]
        if self.index400 != None:
            self.value400 = row[self.index400]
        if self.index402 != None:
            self.value402 = row[self.index402]
        if self.index405 != None:
            self.value405 = row[self.index405]
        if self.index407 != None:
            self.value407 = row[self.index407]
        if self.index410 != None:
            self.value410 = row[self.index410]
        if self.index412 != None:
            self.value412 = row[self.index412]
        if self.index415 != None:
            self.value415 = row[self.index415]
        if self.index417 != None:
            self.value417 = row[self.index417]
        if self.index420 != None:
            self.value420 = row[self.index420]
        if self.index422 != None:
            self.value422 = row[self.index422]
        if self.index425 != None:
            self.value425 = row[self.index425]
        if self.index427 != None:
            self.value427 = row[self.index427]
        if self.index430 != None:
            self.value430 = row[self.index430]
        if self.index432 != None:
            self.value432 = row[self.index432]
        if self.index435 != None:
            self.value435 = row[self.index435]
        if self.index437 != None:
            self.value437 = row[self.index437]
        if self.index440 != None:
            self.value440 = row[self.index440]
        if self.index442 != None:
            self.value442 = row[self.index442]
        if self.index445 != None:
            self.value445 = row[self.index445]
        if self.index447 != None:
            self.value447 = row[self.index447]
        if self.index450 != None:
            self.value450 = row[self.index450]
        if self.index452 != None:
            self.value452 = row[self.index452]
        if self.index455 != None:
            self.value455 = row[self.index455]
        if self.index457 != None:
            self.value457 = row[self.index457]
        if self.index460 != None:
            self.value460 = row[self.index460]
        if self.index462 != None:
            self.value462 = row[self.index462]
        if self.index465 != None:
            self.value465 = row[self.index465]
        if self.index467 != None:
            self.value467 = row[self.index467]
        if self.index470 != None:
            self.value470 = row[self.index470]
        if self.index472 != None:
            self.value472 = row[self.index472]
        if self.index475 != None:
            self.value475 = row[self.index475]
        if self.index477 != None:
            self.value477 = row[self.index477]
        if self.index480 != None:
            self.value480 = row[self.index480]
        if self.index482 != None:
            self.value482 = row[self.index482]
        if self.index485 != None:
            self.value485 = row[self.index485]
        if self.index487 != None:
            self.value487 = row[self.index487]
        if self.index490 != None:
            self.value490 = row[self.index490]
        if self.index492 != None:
            self.value492 = row[self.index492]
        if self.index495 != None:
            self.value495 = row[self.index495]
        if self.index497 != None:
            self.value497 = row[self.index497]
        if self.index500 != None:
            self.value500 = row[self.index500]
        if self.index502 != None:
            self.value502 = row[self.index502]
        if self.index505 != None:
            self.value505 = row[self.index505]
        if self.index507 != None:
            self.value507 = row[self.index507]
        if self.index510 != None:
            self.value510 = row[self.index510]
        if self.index512 != None:
            self.value512 = row[self.index512]
        if self.index515 != None:
            self.value515 = row[self.index515]
        if self.index517 != None:
            self.value517 = row[self.index517]
        if self.index520 != None:
            self.value520 = row[self.index520]
        if self.index522 != None:
            self.value522 = row[self.index522]
        if self.index525 != None:
            self.value525 = row[self.index525]
        if self.index527 != None:
            self.value527 = row[self.index527]
        if self.index530 != None:
            self.value530 = row[self.index530]
        if self.index532 != None:
            self.value532 = row[self.index532]
        if self.index535 != None:
            self.value535 = row[self.index535]
        if self.index537 != None:
            self.value537 = row[self.index537]
        if self.index540 != None:
            self.value540 = row[self.index540]
        if self.index542 != None:
            self.value542 = row[self.index542]
        if self.index545 != None:
            self.value545 = row[self.index545]
        if self.index547 != None:
            self.value547 = row[self.index547]
        if self.index550 != None:
            self.value550 = row[self.index550]
        if self.index552 != None:
            self.value552 = row[self.index552]
        if self.index555 != None:
            self.value555 = row[self.index555]
        if self.index557 != None:
            self.value557 = row[self.index557]
        if self.index560 != None:
            self.value560 = row[self.index560]
        if self.index562 != None:
            self.value562 = row[self.index562]
        if self.index565 != None:
            self.value565 = row[self.index565]
        if self.index567 != None:
            self.value567 = row[self.index567]
        if self.index570 != None:
            self.value570 = row[self.index570]
        if self.index572 != None:
            self.value572 = row[self.index572]
        if self.index575 != None:
            self.value575 = row[self.index575]
        if self.index577 != None:
            self.value577 = row[self.index577]
        if self.index580 != None:
            self.value580 = row[self.index580]
        if self.index582 != None:
            self.value582 = row[self.index582]
        if self.index585 != None:
            self.value585 = row[self.index585]
        if self.index587 != None:
            self.value587 = row[self.index587]
        if self.index590 != None:
            self.value590 = row[self.index590]
        if self.index592 != None:
            self.value592 = row[self.index592]
        if self.index595 != None:
            self.value595 = row[self.index595]
        if self.index597 != None:
            self.value597 = row[self.index597]
        if self.index600 != None:
            self.value600 = row[self.index600]
        if self.index602 != None:
            self.value602 = row[self.index602]
        if self.index605 != None:
            self.value605 = row[self.index605]
        if self.index607 != None:
            self.value607 = row[self.index607]
        if self.index610 != None:
            self.value610 = row[self.index610]
        if self.index612 != None:
            self.value612 = row[self.index612]
        if self.index615 != None:
            self.value615 = row[self.index615]
        if self.index617 != None:
            self.value617 = row[self.index617]
        if self.index620 != None:
            self.value620 = row[self.index620]
        if self.index622 != None:
            self.value622 = row[self.index622]
        if self.index625 != None:
            self.value625 = row[self.index625]
        if self.index627 != None:
            self.value627 = row[self.index627]
        if self.index630 != None:
            self.value630 = row[self.index630]
        if self.index632 != None:
            self.value632 = row[self.index632]
        if self.index635 != None:
            self.value635 = row[self.index635]
        if self.index637 != None:
            self.value637 = row[self.index637]
        if self.index640 != None:
            self.value640 = row[self.index640]
        if self.index642 != None:
            self.value642 = row[self.index642]
        if self.index645 != None:
            self.value645 = row[self.index645]
        if self.index647 != None:
            self.value647 = row[self.index647]
        if self.index650 != None:
            self.value650 = row[self.index650]
        if self.index652 != None:
            self.value652 = row[self.index652]
        if self.index655 != None:
            self.value655 = row[self.index655]
        if self.index657 != None:
            self.value657 = row[self.index657]
        if self.index660 != None:
            self.value660 = row[self.index660]
        if self.index662 != None:
            self.value662 = row[self.index662]
        if self.index665 != None:
            self.value665 = row[self.index665]
        if self.index667 != None:
            self.value667 = row[self.index667]
        if self.index670 != None:
            self.value670 = row[self.index670]
        if self.index672 != None:
            self.value672 = row[self.index672]
        if self.index675 != None:
            self.value675 = row[self.index675]
        if self.index677 != None:
            self.value677 = row[self.index677]
        if self.index680 != None:
            self.value680 = row[self.index680]
        if self.index682 != None:
            self.value682 = row[self.index682]
        if self.index685 != None:
            self.value685 = row[self.index685]
        if self.index687 != None:
            self.value687 = row[self.index687]
        if self.index690 != None:
            self.value690 = row[self.index690]
        if self.index692 != None:
            self.value692 = row[self.index692]
        if self.index695 != None:
            self.value695 = row[self.index695]
        if self.index697 != None:
            self.value697 = row[self.index697]
        if self.index700 != None:
            self.value700 = row[self.index700]
        if self.index702 != None:
            self.value702 = row[self.index702]
        if self.index705 != None:
            self.value705 = row[self.index705]
        if self.index707 != None:
            self.value707 = row[self.index707]
        if self.index710 != None:
            self.value710 = row[self.index710]
        if self.index712 != None:
            self.value712 = row[self.index712]
        if self.index715 != None:
            self.value715 = row[self.index715]
        if self.index717 != None:
            self.value717 = row[self.index717]
        if self.index720 != None:
            self.value720 = row[self.index720]
        if self.index722 != None:
            self.value722 = row[self.index722]
        if self.index725 != None:
            self.value725 = row[self.index725]
        if self.index727 != None:
            self.value727 = row[self.index727]
        if self.index730 != None:
            self.value730 = row[self.index730]
        if self.index732 != None:
            self.value732 = row[self.index732]
        if self.index735 != None:
            self.value735 = row[self.index735]
        if self.index737 != None:
            self.value737 = row[self.index737]
        if self.index740 != None:
            self.value740 = row[self.index740]
        if self.index742 != None:
            self.value742 = row[self.index742]
        if self.index745 != None:
            self.value745 = row[self.index745]
        if self.index747 != None:
            self.value747 = row[self.index747]
        if self.index750 != None:
            self.value750 = row[self.index750]


