# FILE > SETTINGS > Project Interpreter > PyQt5 (Automatic GUI library)
# > pandas = dataframe library
# > numpy
# > sklearn
# > scipy
# > matplotLib
# > xlrd
from PyQt5 import QtCore, QtGui, QtWidgets
from PyQt5.QtWidgets import QFileDialog
from Database import *
from EasterEggs import *
import time


class FilesListWidget(QtWidgets.QListWidget):
    def __init__(self, type, parent=None):
        super(FilesListWidget, self).__init__(parent)
        self.setAcceptDrops(True)
        self.setViewMode(QtWidgets.QListView.IconMode)
        self.setIconSize(QtWidgets.QAbstractItemView.size(self))

        self.optionsDict = {
            "calculateStandardCurve": False,
            "calculateDischarge": False,
            "includeSynoptic": False,
            "interpolate": False,
            "interpolateFrequency": False,
            "include_batch_id": False,
            "correct_values": False,
        }

    def dragEnterEvent(self, event):
        if event.mimeData().hasUrls:
            event.accept()
        else:
            event.ignore()

    def dragMoveEvent(self, event):
        if event.mimeData().hasUrls:
            event.setDropAction(QtCore.Qt.CopyAction)
            event.accept()
        else:
            event.ignore()

    def dropEvent(self, event):
        if event.mimeData().hasUrls:
            event.setDropAction(QtCore.Qt.CopyAction)
            event.accept()
            links = []
            for url in event.mimeData().urls():
                links.append(str(url.toLocalFile()))
            # self.emit(QtCore.PYQT_SIGNAL("dropped"), links)

            self.addItems(links)
            self.repaint()
        else:
            event.setDropAction(QtCore.Qt.MoveAction)
            super(FilesListWidget, self).dropEvent(event)


class Ui_DataTiger(object):
    def __init__(self):
        self.testDict = {
            "fieldSheetInfo": False,
            "measuredDischarge": False,
            "hoboPressure": False,
            "hoboConductivity": False,
            "hoboLight": False,
            "hoboOxygen": False,
            "eureka": False,
            "hanna": False,
            "scanCalculated": False,
            "scanRaw": False,
            "elementar": False,
            "ic": False,
            "icp": False,
            "aqualog": False,
            "invertibrates": False,
            "eDNA": False
        }

    def uploadAllFiles(self):
        if self.db.databaseOpen:
            project = str(self.selectProjectDropdown.currentText())
            if not "no project" in project.lower():
                self.db.changeProjectId(project)

            i = 0
            if self.allowDuplicatesRadio.isChecked():
                self.db.allowDuplicates = True
            else:
                self.db.allowDuplicates = False

            items = []
            for index in range(self.filesList.count()):
                items.append(self.filesList.item(index))

            for item in items:
                path = item.text()
                # try:
                result = self.db.uploadFile(path)
                # except:
                #     result = "ERROR: unable to upload file.\n\n"
                #     result = result + self.easterEgg.pickRandomCondolence(self.horizontalSlider.value())
                self.filesList.takeItem(self.filesList.row(item))
                self.statusUpdateText.append(result)

                self.populateProjects()

        else:

            items = []
            for index in range(self.filesList.count()):
                items.append(self.filesList.item(index))

            for item in items:
                self.filesList.takeItem(self.filesList.row(item))

            result = "Invalid Database. Unable to upload any files.\n\n" + self.easterEgg.pickRandomCondolence(
                self.db.sassCoefficient)
            self.statusUpdateText.append(result)

    def removeSelectedFiles(self):

        selectedItems = self.filesList.selectedItems()
        for item in selectedItems:
            self.filesList.takeItem(self.filesList.row(item))

    def downloadSortChems(self):
        if self.db.databaseOpen:
            # get the folder to save to
            fileName = QtWidgets.QFileDialog.getSaveFileName(self.mainWindow, "Save File", '/', '.csv')[0]

            # get the number of sort-chems to generate
            howMany = self.enterHowMany.toPlainText()

            result = self.db.newSortChems(fileName, howMany)
            self.statusUpdateText.append(result)
        else:
            self.statusUpdateText.append("Unable to generate or download sort chems. Invalid database.\n\n")

    def downloadStandardCurveReport(self):
        if self.db.databaseOpen:

            self.testDict["fieldSheetInfo"] = False
            self.testDict["measuredDischarge"] = False
            self.testDict["hoboPressure"] = True
            self.testDict["hoboConductivity"] = False
            self.testDict["hoboLight"] = False
            self.testDict["hoboOxygen"] = False
            self.testDict["eureka"] = False
            self.testDict["hanna"] = False
            self.testDict["scanCalculated"] = False
            self.testDict["scanRaw"] = False
            self.testDict["elementar"] = False
            self.testDict["ic"] = False
            self.testDict["icp"] = False
            self.testDict["aqualog"] = False
            self.testDict["invertibrates"] = False
            self.testDict["eDNA"] = False

            self.optionsDict = {
                "calculateStandardCurve": True,
                "calculateDischarge": False,
                "includeSynoptic": False,
                "interpolate": False,
                "interpolateFrequency": False,
                "include_batch_id": True,
                "correct_values": False,
            }

            folderName = str(QtWidgets.QFileDialog.getExistingDirectory(self.mainWindow, "Select A Directory"))
            result = self.db.writeStandardCurveReport(folderName, self.testDict, self.optionsDict)
            self.statusUpdateText.append(result)
        else:
            self.statusUpdateText.append("Unable to download missing analyzer tests. Invalid database.\n\n")

    def sendToUVU(self):
        # FIXME! edit which files are to be sent to UVU
        # get the folder to save to
        if self.db.databaseOpen:
            fileName = str(QtWidgets.QFileDialog.getExistingDirectory(self.mainWindow, "Select A Directory"))

            # get the truth value of check buttons
            fieldSheetInfo = self.FieldSheetCheck.isChecked()
            measuredDischarge = self.MeasuredDischargeCheck.isChecked()
            hoboPressure = self.HoboPressureCheck.isChecked()
            hoboConductivity = self.HoboConductivityCheck.isChecked()
            hoboLight = self.HoboLightCheck.isChecked()
            hoboOxygen = self.HoboOxygenCheck.isChecked()
            eureka = self.EurekaCheck.isChecked()
            hanna = self.HannaCheck.isChecked()
            scanCalculated = self.ScanCaclulatedCheck.isChecked()
            scanRaw = self.ScanRawCheck.isChecked()
            elementar = self.ElementarCheck.isChecked()
            ic = self.ICCheck.isChecked()
            icp = self.ICPCheck.isChecked()
            aqualog = self.AqualogCheck.isChecked()
            invertibrates = self.InvertibratesCheck.isChecked()
            eDNA = self.eDNADiversityCheck.isChecked()

            calculateDischarge = self.CalculateDischargeAndConcentrationsCheck.isChecked()
            includeSynoptic = self.IncludeSynopticCheck.isChecked()
            linearlyInterpolate = self.LinearInterpolateCheck.isChecked()
            frequencyInterpolate = self.FrequencyInterpolateCheck.isChecked()

            self.testDict["fieldSheetInfo"] = fieldSheetInfo
            self.testDict["measuredDischarge"] = measuredDischarge
            self.testDict["hoboPressure"] = hoboPressure
            self.testDict["hoboConductivity"] = hoboConductivity
            self.testDict["hoboLight"] = hoboLight
            self.testDict["hoboOxygen"] = hoboOxygen
            self.testDict["eureka"] = eureka
            self.testDict["hanna"] = hanna
            self.testDict["scanCalculated"] = scanCalculated
            self.testDict["scanRaw"] = scanRaw
            self.testDict["elementar"] = elementar
            self.testDict["ic"] = ic
            self.testDict["icp"] = icp
            self.testDict["aqualog"] = aqualog
            self.testDict["invertibrates"] = invertibrates
            self.testDict["eDNA"] = eDNA

            self.optionsDict = {
                "calculateStandardCurve": False,
                "calculateDischarge": calculateDischarge,
                "includeSynoptic": includeSynoptic,
                "interpolate": linearlyInterpolate,
                "interpolateFrequency": frequencyInterpolate,
                "include_batch_id": False,
                "correct_values": False,
            }

            result = self.db.writeTimeSeriesReport(fileName, self.testDict, self.optionsDict)

            self.statusUpdateText.append(result)
        else:
            self.statusUpdateText.append("Unable to download missing analyzer tests. Invalid database.\n\n")

    def downloadTimeSeriesReport(self):
        # get the folder to save to
        if self.db.databaseOpen:
            fileName = str(QtWidgets.QFileDialog.getExistingDirectory(self.mainWindow, "Select A Directory"))

            # # get the truth value of check buttons
            # fieldSheetInfo = self.FieldSheetCheck.isChecked()
            # measuredDischarge = self.MeasuredDischargeCheck.isChecked()
            # hoboPressure = self.HoboPressureCheck.isChecked()
            # hoboConductivity = self.HoboConductivityCheck.isChecked()
            # hoboLight = self.HoboLightCheck.isChecked()
            # hoboOxygen = self.HoboOxygenCheck.isChecked()
            # eureka = self.EurekaCheck.isChecked()
            # hanna = self.HannaCheck.isChecked()
            # scanCalculated = self.ScanCaclulatedCheck.isChecked()
            # scanRaw = self.ScanRawCheck.isChecked()
            # elementar = self.ElementarCheck.isChecked()
            # ic = self.ICCheck.isChecked()
            # icp = self.ICPCheck.isChecked()
            # aqualog = self.AqualogCheck.isChecked()
            # invertibrates = self.InvertibratesCheck.isChecked()
            # eDNA = self.eDNADiversityCheck.isChecked()

            fieldSheetInfo = True
            measuredDischarge = True
            hoboPressure = True
            hoboConductivity = True
            hoboLight = True
            hoboOxygen = True
            eureka = True
            hanna = True
            scanCalculated = True
            scanRaw = True
            elementar = True
            ic = True
            icp = True
            aqualog = True
            invertibrates = True
            eDNA = True

            calculateDischarge = True
            includeSynoptic = True
            linearlyInterpolate = True
            frequencyInterpolate = True

            calculateDischarge = self.CalculateDischargeAndConcentrationsCheck.isChecked()
            includeSynoptic = self.IncludeSynopticCheck.isChecked()
            linearlyInterpolate = self.LinearInterpolateCheck.isChecked()
            frequencyInterpolate = self.FrequencyInterpolateCheck.isChecked()

            self.testDict["fieldSheetInfo"] = fieldSheetInfo
            self.testDict["measuredDischarge"] = measuredDischarge
            self.testDict["hoboPressure"] = hoboPressure
            self.testDict["hoboConductivity"] = hoboConductivity
            self.testDict["hoboLight"] = hoboLight
            self.testDict["hoboOxygen"] = hoboOxygen
            self.testDict["eureka"] = eureka
            self.testDict["hanna"] = hanna
            self.testDict["scanCalculated"] = scanCalculated
            self.testDict["scanRaw"] = scanRaw
            self.testDict["elementar"] = elementar
            self.testDict["ic"] = ic
            self.testDict["icp"] = icp
            self.testDict["aqualog"] = aqualog
            self.testDict["invertibrates"] = invertibrates
            self.testDict["eDNA"] = eDNA

            self.optionsDict = {
                "calculateStandardCurve": False,
                "calculateDischarge": calculateDischarge,
                "includeSynoptic": includeSynoptic,
                "interpolate": linearlyInterpolate,
                "interpolateFrequency": frequencyInterpolate,
                "include_batch_id": False,
                "correct_values": True,
            }

            result = self.db.writeTimeSeriesReport(fileName, self.testDict, self.optionsDict)

            self.statusUpdateText.append(result)
        else:
            self.statusUpdateText.append("Unable to download missing analyzer tests. Invalid database.\n\n")

    def downloadMissingTestsReport(self):
        # get the folder to save to
        if self.db.databaseOpen:
            fileName = str(QtWidgets.QFileDialog.getSaveFileName(self.mainWindow, "Save File", '/', '.csv')[0]) + ".csv"
            result = self.db.writeMissingTestsReport(fileName)

            self.statusUpdateText.append(result)
        else:
            self.statusUpdateText.append("Unable to download missing analyzer tests. Invalid database.\n\n")

    def downloadLoggerGapsReport(self):
        for test in self.testDict.keys():
            # extract only the high frequency loging data
            if test == "hanna" or test == "eureka" or test == "hoboPressure" or test == "hoboConductivity" or test == "hoboOxygen" or test == "hoboLight":
                self.testDict[test] = True
            else:
                self.testDict[test] = False

        # get the folder to save to
        if self.db.databaseOpen:
            fileName = str(QtWidgets.QFileDialog.getExistingDirectory(self.mainWindow, "Select A Directory"))

            self.optionsDict = {
                "calculateStandardCurve": False,
                "calculateDischarge": False,
                "includeSynoptic": False,
                "interpolate": False,
                "interpolateFrequency": False,
                "include_batch_id": False,
                "correct_values": False,
            }

            result = self.db.writeLoggerGapsReport(fileName, self.testDict, self.optionsDict)

            self.statusUpdateText.append(result)
        else:
            self.statusUpdateText.append("Unable to download logger gaps report. Invalid database.\n\n")

    def changeDB(self):
        # get the filename
        dbFile = self.enterNewDB.toPlainText()

        message = self.db.changeDBFile(dbFile)
        self.enterNewDB.setText(message)
        if not "ERROR" in message:
            self.populateProjects()

            self.statusUpdateText.clear()
            self.statusUpdateText.append(self.easterEgg.pickRandomSaying())

    def populateProjects(self):
        self.selectProjectDropdown.clear()

        try:
            projects = self.db.getProjects()
            if len(projects) == 0:
                self.selectProjectDropdown.addItem("No Projects in Database")
            else:
                for project in projects:
                    self.selectProjectDropdown.addItem(project[0])
        except:
            self.selectProjectDropdown.addItem("Unable To access database.")

    def changeSass(self):
        self.db.changeSassCoefficient(self.horizontalSlider.value())

    def setupUi(self, DataTiger, db):
        self.db = db
        self.easterEgg = EasterEggs()

        DataTiger.setObjectName("DataTiger")
        DataTiger.resize(1080, 865)
        self.mainWindow = DataTiger

        self.centralwidget = QtWidgets.QWidget(DataTiger)
        self.centralwidget.setObjectName("centralwidget")

        self.tabWidget = QtWidgets.QTabWidget(self.centralwidget)
        self.tabWidget.setGeometry(QtCore.QRect(0, 0, 1078, 863))
        self.tabWidget.setObjectName("tabWidget")

        self.tab = QtWidgets.QWidget()
        self.tab.setObjectName("tab")

        self.DownloadSortChems = QtWidgets.QPushButton(self.tab)
        self.DownloadSortChems.setGeometry(QtCore.QRect(230, 750, 151, 28))
        self.DownloadSortChems.clicked.connect(self.downloadSortChems)
        self.DownloadSortChems.setObjectName("DownloadSortChems")

        self.enterHowMany = QtWidgets.QTextEdit(self.tab)
        self.enterHowMany.setGeometry(QtCore.QRect(240, 710, 141, 31))
        self.enterHowMany.setObjectName("enterHowMany")

        self.howManyLabel = QtWidgets.QLabel(self.tab)
        self.howManyLabel.setGeometry(QtCore.QRect(230, 670, 161, 31))
        self.howManyLabel.setObjectName("howManyLabel")

        self.sortChemMakerLabel = QtWidgets.QTextBrowser(self.tab)
        self.sortChemMakerLabel.setGeometry(QtCore.QRect(230, 600, 151, 71))
        self.sortChemMakerLabel.setObjectName("sortChemMakerLabel")

        self.downloadMissingTestsButton = QtWidgets.QPushButton(self.tab)
        self.downloadMissingTestsButton.setGeometry(QtCore.QRect(10, 70, 121, 28))
        self.downloadMissingTestsButton.clicked.connect(self.downloadMissingTestsReport)
        self.downloadMissingTestsButton.setObjectName("downloadMissingTestsButton")

        self.FileUploadTool = QtWidgets.QTextBrowser(self.tab)
        self.FileUploadTool.setGeometry(QtCore.QRect(600, 10, 256, 61))
        self.FileUploadTool.setObjectName("FileUploadTool")

        self.statusUpdateText = QtWidgets.QTextBrowser(self.tab)
        self.statusUpdateText.setGeometry(QtCore.QRect(410, 610, 641, 171))
        self.statusUpdateText.setObjectName("statusUpdateText")

        if not self.db.databaseOpen:
            self.statusUpdateText.append("Invalid Database.\n\n")
        else:
            self.statusUpdateText.append(self.easterEgg.pickRandomSaying())

        self.statusUpdateLabel = QtWidgets.QLabel(self.tab)
        self.statusUpdateLabel.setGeometry(QtCore.QRect(690, 580, 91, 31))
        self.statusUpdateLabel.setObjectName("statusUpdateLabel")

        self.horizontalLine = QtWidgets.QFrame(self.tab)
        self.horizontalLine.setGeometry(QtCore.QRect(410, 570, 641, 21))
        self.horizontalLine.setFrameShape(QtWidgets.QFrame.HLine)
        self.horizontalLine.setFrameShadow(QtWidgets.QFrame.Sunken)
        self.horizontalLine.setObjectName("horizontalLine")

        self.verticalLine = QtWidgets.QFrame(self.tab)
        self.verticalLine.setGeometry(QtCore.QRect(370, -20, 41, 801))
        self.verticalLine.setFrameShape(QtWidgets.QFrame.VLine)
        self.verticalLine.setFrameShadow(QtWidgets.QFrame.Sunken)
        self.verticalLine.setObjectName("verticalLine")

        self.MissingTestsLabel = QtWidgets.QTextBrowser(self.tab)
        self.MissingTestsLabel.setGeometry(QtCore.QRect(10, 20, 261, 41))
        self.MissingTestsLabel.setObjectName("MissingTestsLabel")

        self.verticalLayoutWidget = QtWidgets.QWidget(self.tab)
        self.verticalLayoutWidget.setGeometry(QtCore.QRect(410, 80, 641, 491))
        self.verticalLayoutWidget.setObjectName("verticalLayoutWidget")

        self.verticalLayout_3 = QtWidgets.QVBoxLayout(self.verticalLayoutWidget)
        self.verticalLayout_3.setContentsMargins(0, 0, 0, 0)
        self.verticalLayout_3.setObjectName("verticalLayout_3")

        self.selectProjectLabel_2 = QtWidgets.QLabel(self.verticalLayoutWidget)
        self.selectProjectLabel_2.setAlignment(QtCore.Qt.AlignCenter)
        self.selectProjectLabel_2.setObjectName("selectProjectLabel_2")
        self.verticalLayout_3.addWidget(self.selectProjectLabel_2)

        self.selectProjectDropdown = QtWidgets.QComboBox(self.verticalLayoutWidget)
        self.selectProjectDropdown.setObjectName("selectProjectDropdown")
        self.selectProjectDropdown.addItem("")
        self.populateProjects()
        self.verticalLayout_3.addWidget(self.selectProjectDropdown)

        self.dragAndDropFilesLabel = QtWidgets.QLabel(self.verticalLayoutWidget)
        self.dragAndDropFilesLabel.setAlignment(QtCore.Qt.AlignCenter)
        self.dragAndDropFilesLabel.setObjectName("dragAndDropFilesLabel")
        self.verticalLayout_3.addWidget(self.dragAndDropFilesLabel)

        self.filesList = FilesListWidget(QtWidgets.QListWidget(self.verticalLayoutWidget))
        self.filesList.setObjectName("filesList")
        self.verticalLayout_3.addWidget(self.filesList)

        self.allowDuplicatesRadio = QtWidgets.QRadioButton(self.verticalLayoutWidget)
        self.allowDuplicatesRadio.setObjectName("allowDuplicatesRadio")
        self.verticalLayout_3.addWidget(self.allowDuplicatesRadio)

        self.removeSelectedButton = QtWidgets.QPushButton(self.verticalLayoutWidget)
        self.removeSelectedButton.setObjectName("removeSelectedButton")
        self.removeSelectedButton.clicked.connect(self.removeSelectedFiles)
        self.verticalLayout_3.addWidget(self.removeSelectedButton)

        self.submitFilesButton = QtWidgets.QPushButton(self.verticalLayoutWidget)
        self.submitFilesButton.setObjectName("submitFilesButton")
        self.submitFilesButton.clicked.connect(self.uploadAllFiles)
        self.verticalLayout_3.addWidget(self.submitFilesButton)

        self.ExportTimeSeriesLabel = QtWidgets.QTextBrowser(self.tab)
        self.ExportTimeSeriesLabel.setGeometry(QtCore.QRect(10, 200, 251, 41))
        self.ExportTimeSeriesLabel.setObjectName("ExportTimeSeriesLabel")

        self.dragAndDropFilesLabel_2 = QtWidgets.QLabel(self.tab)
        self.dragAndDropFilesLabel_2.setGeometry(QtCore.QRect(-40, 250, 329, 16))
        self.dragAndDropFilesLabel_2.setAlignment(QtCore.Qt.AlignCenter)
        self.dragAndDropFilesLabel_2.setObjectName("dragAndDropFilesLabel_2")

        self.downloadTimeSeriesButton = QtWidgets.QPushButton(self.tab)
        self.downloadTimeSeriesButton.setGeometry(QtCore.QRect(10, 690, 181, 28))
        self.downloadTimeSeriesButton.clicked.connect(self.downloadTimeSeriesReport)
        self.downloadTimeSeriesButton.setObjectName("downloadTimeSeriesButton")

        self.ExportGapsLabel = QtWidgets.QTextBrowser(self.tab)
        self.ExportGapsLabel.setGeometry(QtCore.QRect(10, 110, 291, 41))
        self.ExportGapsLabel.setObjectName("ExportGapsLabel")

        self.downloadGapsButton = QtWidgets.QPushButton(self.tab)
        self.downloadGapsButton.setGeometry(QtCore.QRect(10, 160, 121, 28))
        self.downloadGapsButton.clicked.connect(self.downloadLoggerGapsReport)
        self.downloadGapsButton.setObjectName("downloadGapsButton")

        self.IncludeSynopticCheck = QtWidgets.QCheckBox(self.tab)
        self.IncludeSynopticCheck.setGeometry(QtCore.QRect(10, 620, 171, 20))
        self.IncludeSynopticCheck.setObjectName("IncludeSynopticCheck")

        self.FieldSheetCheck = QtWidgets.QCheckBox(self.tab)
        self.FieldSheetCheck.setGeometry(QtCore.QRect(10, 270, 131, 20))
        self.FieldSheetCheck.setObjectName("FieldSheetCheck")

        self.MeasuredDischargeCheck = QtWidgets.QCheckBox(self.tab)
        self.MeasuredDischargeCheck.setGeometry(QtCore.QRect(10, 290, 151, 20))
        self.MeasuredDischargeCheck.setObjectName("MeasuredDischargeCheck")

        self.HoboPressureCheck = QtWidgets.QCheckBox(self.tab)
        self.HoboPressureCheck.setGeometry(QtCore.QRect(10, 310, 111, 20))
        self.HoboPressureCheck.setObjectName("HoboPressureCheck")

        self.HoboConductivityCheck = QtWidgets.QCheckBox(self.tab)
        self.HoboConductivityCheck.setGeometry(QtCore.QRect(10, 330, 131, 20))
        self.HoboConductivityCheck.setObjectName("HoboConductivityCheck")

        self.HoboLightCheck = QtWidgets.QCheckBox(self.tab)
        self.HoboLightCheck.setGeometry(QtCore.QRect(10, 350, 121, 20))
        self.HoboLightCheck.setObjectName("HoboLightCheck")

        self.HoboOxygenCheck = QtWidgets.QCheckBox(self.tab)
        self.HoboOxygenCheck.setGeometry(QtCore.QRect(10, 370, 111, 20))
        self.HoboOxygenCheck.setObjectName("HoboOxygenCheck")

        self.EurekaCheck = QtWidgets.QCheckBox(self.tab)
        self.EurekaCheck.setGeometry(QtCore.QRect(10, 390, 95, 20))
        self.EurekaCheck.setObjectName("EurekaCheck")

        self.HannaCheck = QtWidgets.QCheckBox(self.tab)
        self.HannaCheck.setGeometry(QtCore.QRect(10, 410, 95, 20))
        self.HannaCheck.setObjectName("HannaCheck")

        self.ElementarCheck = QtWidgets.QCheckBox(self.tab)
        self.ElementarCheck.setGeometry(QtCore.QRect(10, 470, 95, 20))
        self.ElementarCheck.setObjectName("ElementarCheck")

        self.CalculateDischargeAndConcentrationsCheck = QtWidgets.QCheckBox(self.tab)
        self.CalculateDischargeAndConcentrationsCheck.setGeometry(QtCore.QRect(10, 600, 141, 20))
        self.CalculateDischargeAndConcentrationsCheck.setObjectName("CalculateDischargeAndConcentrationsCheck")

        self.FrequencyInterpolateCheck = QtWidgets.QCheckBox(self.tab)
        self.FrequencyInterpolateCheck.setGeometry(QtCore.QRect(10, 660, 181, 20))
        self.FrequencyInterpolateCheck.setObjectName("FrequencyInterpolateCheck")

        self.ICCheck = QtWidgets.QCheckBox(self.tab)
        self.ICCheck.setGeometry(QtCore.QRect(10, 490, 95, 20))
        self.ICCheck.setObjectName("ICCheck")

        self.ICPCheck = QtWidgets.QCheckBox(self.tab)
        self.ICPCheck.setGeometry(QtCore.QRect(10, 510, 95, 20))
        self.ICPCheck.setObjectName("ICPCheck")

        self.ScanCaclulatedCheck = QtWidgets.QCheckBox(self.tab)
        self.ScanCaclulatedCheck.setGeometry(QtCore.QRect(10, 430, 131, 20))
        self.ScanCaclulatedCheck.setObjectName("ScanCaclulatedCheck")

        self.LinearInterpolateCheck = QtWidgets.QCheckBox(self.tab)
        self.LinearInterpolateCheck.setGeometry(QtCore.QRect(10, 640, 171, 20))
        self.LinearInterpolateCheck.setObjectName("InterpolateCheck")

        self.ScanRawCheck = QtWidgets.QCheckBox(self.tab)
        self.ScanRawCheck.setGeometry(QtCore.QRect(10, 450, 95, 20))
        self.ScanRawCheck.setObjectName("ScanRawCheck")

        self.InvertibratesCheck = QtWidgets.QCheckBox(self.tab)
        self.InvertibratesCheck.setGeometry(QtCore.QRect(10, 550, 101, 20))
        self.InvertibratesCheck.setObjectName("InvertibratesCheck")

        self.eDNADiversityCheck = QtWidgets.QCheckBox(self.tab)
        self.eDNADiversityCheck.setGeometry(QtCore.QRect(10, 570, 111, 20))
        self.eDNADiversityCheck.setObjectName("eDNADiversityCheck")

        self.AqualogCheck = QtWidgets.QCheckBox(self.tab)
        self.AqualogCheck.setGeometry(QtCore.QRect(10, 530, 95, 20))
        self.AqualogCheck.setObjectName("AqualogCheck")

        self.sendToUVUButton = QtWidgets.QPushButton(self.tab)
        self.sendToUVUButton.setGeometry(QtCore.QRect(10, 770, 181, 28))
        self.sendToUVUButton.clicked.connect(self.sendToUVU)
        self.sendToUVUButton.setObjectName("sendToUVUButton")

        self.calculateStandardCurveButton = QtWidgets.QPushButton(self.tab)
        self.calculateStandardCurveButton.setGeometry(QtCore.QRect(10, 730, 181, 28))
        self.calculateStandardCurveButton.clicked.connect(self.downloadStandardCurveReport)
        self.calculateStandardCurveButton.setObjectName("calculateStandardCurveButton")

        self.tabWidget.addTab(self.tab, "")

        self.tab_2 = QtWidgets.QWidget()
        self.tab_2.setObjectName("tab_2")

        self.enterNewDB = QtWidgets.QTextEdit(self.tab_2)
        self.enterNewDB.setGeometry(QtCore.QRect(10, 110, 331, 131))
        self.enterNewDB.setObjectName("enterNewDB")

        self.changeSQLlabel = QtWidgets.QLabel(self.tab_2)
        self.changeSQLlabel.setGeometry(QtCore.QRect(10, 90, 221, 16))
        self.changeSQLlabel.setObjectName("changeSQLlabel")

        self.changeDBButton = QtWidgets.QPushButton(self.tab_2)
        self.changeDBButton.setGeometry(QtCore.QRect(10, 250, 151, 31))
        self.changeDBButton.clicked.connect(self.changeDB)
        self.changeDBButton.setObjectName("changeDBButton")

        self.changeSQLfile = QtWidgets.QTextBrowser(self.tab_2)
        self.changeSQLfile.setGeometry(QtCore.QRect(10, 40, 331, 41))
        self.changeSQLfile.setObjectName("changeSQLfile")

        self.horizontalSlider = QtWidgets.QSlider(self.tab_2)
        self.horizontalSlider.setGeometry(QtCore.QRect(10, 360, 321, 22))
        self.horizontalSlider.setOrientation(QtCore.Qt.Horizontal)
        self.horizontalSlider.setObjectName("horizontalSlider")

        self.label = QtWidgets.QLabel(self.tab_2)
        self.label.setGeometry(QtCore.QRect(10, 390, 71, 16))
        self.label.setObjectName("label")

        self.label_2 = QtWidgets.QLabel(self.tab_2)
        self.label_2.setGeometry(QtCore.QRect(260, 390, 71, 16))
        self.label_2.setObjectName("label_2")

        self.textBrowser = QtWidgets.QTextBrowser(self.tab_2)
        self.textBrowser.setGeometry(QtCore.QRect(10, 310, 161, 41))
        self.textBrowser.setObjectName("textBrowser")

        self.changeSassButton = QtWidgets.QPushButton(self.tab_2)
        self.changeSassButton.setGeometry(QtCore.QRect(10, 420, 93, 28))
        self.changeSassButton.clicked.connect(self.changeSass)
        self.changeSassButton.setObjectName("changeSassButton")

        self.tabWidget.addTab(self.tab_2, "")

        DataTiger.setCentralWidget(self.centralwidget)

        self.menubar = QtWidgets.QMenuBar(DataTiger)
        self.menubar.setGeometry(QtCore.QRect(0, 0, 1063, 26))
        self.menubar.setObjectName("menubar")
        DataTiger.setMenuBar(self.menubar)

        self.statusbar = QtWidgets.QStatusBar(DataTiger)
        self.statusbar.setObjectName("statusbar")
        DataTiger.setStatusBar(self.statusbar)

        self.retranslateUi(DataTiger)
        self.tabWidget.setCurrentIndex(0)
        QtCore.QMetaObject.connectSlotsByName(DataTiger)

    def retranslateUi(self, DataTiger):
        _translate = QtCore.QCoreApplication.translate
        DataTiger.setWindowTitle(_translate("DataTiger", "MainWindow"))
        self.DownloadSortChems.setText(_translate("DataTiger", "generate and download"))
        self.enterHowMany.setHtml(_translate("DataTiger",
                                             "<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.0//EN\" \"http://www.w3.org/TR/REC-html40/strict.dtd\">\n"
                                             "<html><head><meta name=\"qrichtext\" content=\"1\" /><style type=\"text/css\">\n"
                                             "p, li { white-space: pre-wrap; }\n"
                                             "</style></head><body style=\" font-family:\'MS Shell Dlg 2\'; font-size:7.8pt; font-weight:400; font-style:normal;\">\n"
                                             "<p style=\"-qt-paragraph-type:empty; margin-top:0px; margin-bottom:0px; margin-left:0px; margin-right:0px; -qt-block-indent:0; text-indent:0px;\"><br /></p></body></html>"))
        self.howManyLabel.setText(_translate("DataTiger", "How many would you like?"))
        self.sortChemMakerLabel.setHtml(_translate("DataTiger",
                                                   "<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.0//EN\" \"http://www.w3.org/TR/REC-html40/strict.dtd\">\n"
                                                   "<html><head><meta name=\"qrichtext\" content=\"1\" /><style type=\"text/css\">\n"
                                                   "p, li { white-space: pre-wrap; }\n"
                                                   "</style></head><body style=\" font-family:\'MS Shell Dlg 2\'; font-size:7.8pt; font-weight:400; font-style:normal;\">\n"
                                                   "<p align=\"center\" style=\" margin-top:0px; margin-bottom:0px; margin-left:0px; margin-right:0px; -qt-block-indent:0; text-indent:0px;\"><span style=\" font-size:14pt; font-weight:600;\">Sort-Chem Maker</span></p></body></html>"))
        self.downloadMissingTestsButton.setText(_translate("DataTiger", "Download as .csv"))
        self.FileUploadTool.setHtml(_translate("DataTiger",
                                               "<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.0//EN\" \"http://www.w3.org/TR/REC-html40/strict.dtd\">\n"
                                               "<html><head><meta name=\"qrichtext\" content=\"1\" /><style type=\"text/css\">\n"
                                               "p, li { white-space: pre-wrap; }\n"
                                               "</style></head><body style=\" font-family:\'MS Shell Dlg 2\'; font-size:7.8pt; font-weight:400; font-style:normal;\">\n"
                                               "<p align=\"center\" style=\" margin-top:0px; margin-bottom:0px; margin-left:0px; margin-right:0px; -qt-block-indent:0; text-indent:0px;\"><span style=\" font-size:14pt; font-weight:600;\">The Tiger\'s Mouth</span></p>\n"
                                               "<p align=\"center\" style=\" margin-top:0px; margin-bottom:0px; margin-left:0px; margin-right:0px; -qt-block-indent:0; text-indent:0px;\"><span style=\" font-size:9pt;\">(File Upload Tool)</span></p></body></html>"))
        self.statusUpdateLabel.setText(_translate("DataTiger", "Status Updates"))
        self.MissingTestsLabel.setHtml(_translate("DataTiger",
                                                  "<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.0//EN\" \"http://www.w3.org/TR/REC-html40/strict.dtd\">\n"
                                                  "<html><head><meta name=\"qrichtext\" content=\"1\" /><style type=\"text/css\">\n"
                                                  "p, li { white-space: pre-wrap; }\n"
                                                  "</style></head><body style=\" font-family:\'MS Shell Dlg 2\'; font-size:7.8pt; font-weight:400; font-style:normal;\">\n"
                                                  "<p style=\" margin-top:0px; margin-bottom:0px; margin-left:0px; margin-right:0px; -qt-block-indent:0; text-indent:0px;\"><span style=\" font-size:14pt; font-weight:600;\">Export Missing Tests</span></p></body></html>"))
        self.selectProjectLabel_2.setText(
            _translate("DataTiger", "Select the default \"Project\" the files belong to."))
        self.selectProjectDropdown.setItemText(0, _translate("DataTiger", "MegaFire"))
        self.dragAndDropFilesLabel.setText(_translate("DataTiger", "Drag and drop the files here."))
        self.sendToUVUButton.setText(_translate("DataTiger", "Send to UVU"))
        self.allowDuplicatesRadio.setText(
            _translate("DataTiger", "Allow Duplicate Files (in the case of data-correction)"))
        self.removeSelectedButton.setText(_translate("DataTiger", "remove selected"))
        self.submitFilesButton.setText(_translate("DataTiger", "submit files"))
        self.ExportTimeSeriesLabel.setHtml(_translate("DataTiger",
                                                      "<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.0//EN\" \"http://www.w3.org/TR/REC-html40/strict.dtd\">\n"
                                                      "<html><head><meta name=\"qrichtext\" content=\"1\" /><style type=\"text/css\">\n"
                                                      "p, li { white-space: pre-wrap; }\n"
                                                      "</style></head><body style=\" font-family:\'MS Shell Dlg 2\'; font-size:7.8pt; font-weight:400; font-style:normal;\">\n"
                                                      "<p style=\" margin-top:0px; margin-bottom:0px; margin-left:0px; margin-right:0px; -qt-block-indent:0; text-indent:0px;\"><span style=\" font-size:14pt; font-weight:600;\">Export Time Series!</span></p></body></html>"))
        self.dragAndDropFilesLabel_2.setText(_translate("DataTiger", "Select the analyers you wish to include"))
        self.downloadTimeSeriesButton.setText(_translate("DataTiger", "Download .csv files to folder"))
        self.IncludeSynopticCheck.setText(_translate("DataTiger", "Include all synoptic sites"))
        self.ExportGapsLabel.setHtml(_translate("DataTiger",
                                                "<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.0//EN\" \"http://www.w3.org/TR/REC-html40/strict.dtd\">\n"
                                                "<html><head><meta name=\"qrichtext\" content=\"1\" /><style type=\"text/css\">\n"
                                                "p, li { white-space: pre-wrap; }\n"
                                                "</style></head><body style=\" font-family:\'MS Shell Dlg 2\'; font-size:7.8pt; font-weight:400; font-style:normal;\">\n"
                                                "<p style=\" margin-top:0px; margin-bottom:0px; margin-left:0px; margin-right:0px; -qt-block-indent:0; text-indent:0px;\"><span style=\" font-size:14pt; font-weight:600;\">Export Gaps In Loggers</span></p></body></html>"))
        self.downloadGapsButton.setText(_translate("DataTiger", "Download as .csv"))
        self.FieldSheetCheck.setText(_translate("DataTiger", "Field Sheet Info"))
        self.MeasuredDischargeCheck.setText(_translate("DataTiger", "Measured Discharge"))
        self.HoboPressureCheck.setText(_translate("DataTiger", "Hobo Pressure"))
        self.HoboConductivityCheck.setText(_translate("DataTiger", "Hobo Conductivity"))
        self.HoboLightCheck.setText(_translate("DataTiger", "Hobo Light"))
        self.HoboOxygenCheck.setText(_translate("DataTiger", "Hobo Oxygen"))
        self.EurekaCheck.setText(_translate("DataTiger", "Eureka"))
        self.HannaCheck.setText(_translate("DataTiger", "Hanna"))
        self.ElementarCheck.setText(_translate("DataTiger", "TOC"))
        self.CalculateDischargeAndConcentrationsCheck.setText(_translate("DataTiger", "Calculate Discharge"))
        self.ICCheck.setText(_translate("DataTiger", "IC"))
        self.ICPCheck.setText(_translate("DataTiger", "ICP"))
        self.ScanCaclulatedCheck.setText(_translate("DataTiger", "Scan - Calculated"))
        self.LinearInterpolateCheck.setText(_translate("DataTiger", "Linearly Interpolate"))
        self.ScanRawCheck.setText(_translate("DataTiger", "Scan - Raw"))
        self.InvertibratesCheck.setText(_translate("DataTiger", "Invertibrates"))
        self.eDNADiversityCheck.setText(_translate("DataTiger", "eDNA Diversity"))
        self.AqualogCheck.setText(_translate("DataTiger", "Aqualog"))
        self.FrequencyInterpolateCheck.setText(_translate("DataTiger", "Frequency Interpolation"))
        self.tabWidget.setTabText(self.tabWidget.indexOf(self.tab), _translate("DataTiger", "Tools"))
        self.calculateStandardCurveButton.setText(_translate("DataTiger", "re-calculate Standard Curves"))
        self.changeSQLlabel.setText(_translate("DataTiger", "Drag and drop the database file here"))
        self.changeDBButton.setText(_translate("DataTiger", "Change Database"))
        self.changeSQLfile.setHtml(_translate("DataTiger",
                                              "<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.0//EN\" \"http://www.w3.org/TR/REC-html40/strict.dtd\">\n"
                                              "<html><head><meta name=\"qrichtext\" content=\"1\" /><style type=\"text/css\">\n"
                                              "p, li { white-space: pre-wrap; }\n"
                                              "</style></head><body style=\" font-family:\'MS Shell Dlg 2\'; font-size:7.8pt; font-weight:400; font-style:normal;\">\n"
                                              "<p style=\" margin-top:0px; margin-bottom:0px; margin-left:0px; margin-right:0px; -qt-block-indent:0; text-indent:0px;\"><span style=\" font-size:12pt; font-weight:600;\">Change SQLite database file</span></p></body></html>"))
        self.label.setText(_translate("DataTiger", "Less Sassy"))
        self.label_2.setText(_translate("DataTiger", "More Sassy"))
        self.textBrowser.setHtml(_translate("DataTiger",
                                            "<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.0//EN\" \"http://www.w3.org/TR/REC-html40/strict.dtd\">\n"
                                            "<html><head><meta name=\"qrichtext\" content=\"1\" /><style type=\"text/css\">\n"
                                            "p, li { white-space: pre-wrap; }\n"
                                            "</style></head><body style=\" font-family:\'MS Shell Dlg 2\'; font-size:7.8pt; font-weight:400; font-style:normal;\">\n"
                                            "<p style=\" margin-top:0px; margin-bottom:0px; margin-left:0px; margin-right:0px; -qt-block-indent:0; text-indent:0px;\"><span style=\" font-size:14pt; font-weight:600;\">Sass Control</span></p></body></html>"))
        self.changeSassButton.setText(_translate("DataTiger", "Apply"))
        self.tabWidget.setTabText(self.tabWidget.indexOf(self.tab_2), _translate("DataTiger", "Settings"))


if __name__ == "__main__":
    import sys

    app = QtWidgets.QApplication(sys.argv)
    DataTiger = QtWidgets.QMainWindow()
    ui = Ui_DataTiger()
    db = Database()
    ui.setupUi(DataTiger, db)
    DataTiger.show()
    sys.exit(app.exec_())