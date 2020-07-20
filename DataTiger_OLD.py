
# -*- coding: utf-8 -*-
# Form implementation generated from reading ui file 'C:\Users\BCBrown\Desktop\Abbott\appDesign1.ui'
#
# Created by: PyQt5 UI code generator 5.11.3
#
# WARNING! All changes made in this file will be lost!

from PyQt5 import QtCore, QtGui, QtWidgets
from PyQt5.QtWidgets import QFileDialog
from Database import *
from EasterEggs import *
import time

# class FileTextWidget(QtWidgets.QTextEdit):
#     def __init__(self, type, parent=None):
#         super(FileTextWidget, self).__init__(parent)
#         self.setAcceptDrops(True)
#
#     def dragEnterEvent(self, event):
#         if event.mimeData().hasUrls:
#             event.accept()
#         else:
#             event.ignore()
#     def dragMoveEvent(self, event):
#         if event.mimeData().hasUrls:
#             event.setDropAction(QtCore.Qt.CopyAction)
#             event.accept()
#         else:
#             event.ignore()
#
#     def dropEvent(self, event):
#         self.clear()
#         if event.mimeData().hasUrls:
#             event.setDropAction(QtCore.Qt.CopyAction)
#             event.accept()
#             links = []
#             for url in event.mimeData().urls():
#                 links.append(str(url.toLocalFile()))
#             # self.emit(QtCore.PYQT_SIGNAL("dropped"), links)
#             self.setText(links[0])
#             # self.addItems(links)
#             # self.repaint()
#         else:
#             event.setDropAction(QtCore.Qt.MoveAction)
#             super(FileTextWidget, self).dropEvent(event)
#
#


class FilesListWidget(QtWidgets.QListWidget):
    def __init__(self, type, parent=None):
        super(FilesListWidget, self).__init__(parent)
        self.setAcceptDrops(True)
        self.setViewMode(QtWidgets.QListView.IconMode)
        self.setIconSize(QtWidgets.QAbstractItemView.size(self))


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
    def uploadAllFiles(self):
         # grab the default project

         if self.db.databaseOpen:
             project = str(self.selectProjectDropdown.currentText())
             if not "no project" in project.lower():
                 self.db.changeProjectId(project)

             i = 0
             if self.radioButton.isChecked():
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

                 self.populateTable()
                 self.populateProjects()

         else:

             items = []
             for index in range(self.filesList.count()):
                 items.append(self.filesList.item(index))

             for item in items:
                 self.filesList.takeItem(self.filesList.row(item))

             result = "Invalid Database. Unable to upload any files.\n\n" + self.easterEgg.pickRandomCondolence(self.db.sassCoefficient)
             self.statusUpdateText.append(result)

    def removeSelectedFiles(self):

         selectedItems = self.filesList.selectedItems()
         for item in selectedItems:
             self.filesList.takeItem(self.filesList.row(item))


    # def saveFileDialog(self):
    #     options = QFileDialog.Options()
    #     options |= QFileDialog.DontUseNativeDialog
    #     fileName, _ = QFileDialog.getSaveFileName(self,"QFileDialog.getSaveFileName()","","All Files (*);;Text Files (*.txt)", options=options)
    #     if fileName:
    #         print(fileName)

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

    def downloadTestsReport(self):
        # get the folder to save to
        if self.db.databaseOpen:
            fileName = str(QtWidgets.QFileDialog.getSaveFileName(self.mainWindow, "Save File", '/', '.csv')[0]) + ".csv"
            result = self.db.writeTestsReport(fileName)

            self.populateTable()
            self.statusUpdateText.append(result)
        else:
            self.statusUpdateText.append("Unable to download missing tiger tests. Invalid database.\n\n")


    def changeDB(self):
        # get the filename
        dbFile = self.enterNewDB.toPlainText()

        message = self.db.changeDBFile(dbFile)
        self.enterNewDB.setText(message)
        if not "ERROR" in message:
            self.populateTable()
            self.populateProjects()

            self.statusUpdateText.clear()
            self.statusUpdateText.append(self.easterEgg.pickRandomSaying())


    def populateTable(self):
        try:
            startTime = time.time()
            sortChemsWithBatches = self.db.analyzeTests()
            self.testsTable.setRowCount(len(sortChemsWithBatches))
            if len(sortChemsWithBatches) > 0:
                self.testsTable.setColumnCount(len(sortChemsWithBatches[0]))
            else:
                self.testsTable.setColumnCount(0)
            i = 0
            for row in sortChemsWithBatches:
                j = 0
                for item in row:

                    qItem = QtWidgets.QTableWidgetItem(str(item))
                    self.testsTable.setItem(i,j,qItem)
                    j = j + 1
                i = i + 1

        except DatabaseTooLong:
            self.testsTable.setRowCount(1)
            self.testsTable.setColumnCount(1)
            qItem = QtWidgets.QTableWidgetItem("Database took too long to load. Consider checking your connection with box.")
            self.testsTable.setItem(0,0,qItem)
        except:
            self.testsTable.setRowCount(1)
            self.testsTable.setColumnCount(1)
            qItem = QtWidgets.QTableWidgetItem("Error accessing database: " + self.db.defaultDBFile)
            self.testsTable.setItem(0,0,qItem)

    def populateProjects(self):
            self.selectProjectDropdown.clear()

        # try:
            projects = self.db.getProjects()
            if len(projects) == 0:
                self.selectProjectDropdown.addItem("No Projects in Database")
            else:
                for project in projects:
                    self.selectProjectDropdown.addItem(project[0])
        # except:
        #     self.selectProjectDropdown.addItem("Unable To access database.")


    def changeSass(self):
        self.db.changeSassCoefficient(self.horizontalSlider.value())

    def setupUi(self, DataTiger, db):

        self.db = db
        self.easterEgg = EasterEggs()

        DataTiger.setObjectName("DataTiger")
        DataTiger.resize(1063, 865)
        self.mainWindow = DataTiger

        self.centralwidget = QtWidgets.QWidget(DataTiger)
        self.centralwidget.setObjectName("centralwidget")

        self.tabWidget = QtWidgets.QTabWidget(self.centralwidget)
        self.tabWidget.setGeometry(QtCore.QRect(0, 0, 1061, 811))
        self.tabWidget.setObjectName("tabWidget")

        self.tab = QtWidgets.QWidget()
        self.tab.setObjectName("tab")

        self.pushButton = QtWidgets.QPushButton(self.tab)
        self.pushButton.setGeometry(QtCore.QRect(10, 720, 151, 28))
        self.pushButton.clicked.connect(self.downloadSortChems)
        self.pushButton.setObjectName("pushButton")

        self.enterHowMany = QtWidgets.QTextEdit(self.tab)
        self.enterHowMany.setGeometry(QtCore.QRect(10, 680, 141, 31))
        self.enterHowMany.setObjectName("enterHowMany")

        self.howManyLabel = QtWidgets.QLabel(self.tab)
        self.howManyLabel.setGeometry(QtCore.QRect(10, 650, 161, 31))
        self.howManyLabel.setObjectName("howManyLabel")

        self.sortChemMaker = QtWidgets.QTextBrowser(self.tab)
        self.sortChemMaker.setGeometry(QtCore.QRect(10, 610, 241, 41))
        self.sortChemMaker.setObjectName("sortChemMaker")

        self.downloadTestsButton = QtWidgets.QPushButton(self.tab)
        self.downloadTestsButton.setGeometry(QtCore.QRect(580, 590, 121, 28))
        self.downloadTestsButton.clicked.connect(self.downloadTestsReport)
        self.downloadTestsButton.setObjectName("downloadTestsButton")

        self.testsTable = QtWidgets.QTableWidget(self.tab)
        self.testsTable.setGeometry(QtCore.QRect(10, 60, 691, 521))
        self.testsTable.setObjectName("testsTable")
        self.testsTable.setColumnCount(0)
        self.testsTable.setRowCount(0)

        self.populateTable()

        self.FileUploadTool = QtWidgets.QTextBrowser(self.tab)
        self.FileUploadTool.setGeometry(QtCore.QRect(750, 10, 256, 61))
        self.FileUploadTool.setObjectName("FileUploadTool")

        self.statusUpdateText = QtWidgets.QTextBrowser(self.tab)
        self.statusUpdateText.setGeometry(QtCore.QRect(720, 610, 331, 171))

        if not self.db.databaseOpen:
            self.statusUpdateText.append("Invalid Database.\n\n")
        else:
            self.statusUpdateText.append(self.easterEgg.pickRandomSaying())
        self.statusUpdateText.setObjectName("statusUpdateText")

        self.statusUpdateLabel = QtWidgets.QLabel(self.tab)
        self.statusUpdateLabel.setGeometry(QtCore.QRect(840, 580, 91, 31))
        self.statusUpdateLabel.setObjectName("statusUpdateLabel")

        self.horizontalLine = QtWidgets.QFrame(self.tab)
        self.horizontalLine.setGeometry(QtCore.QRect(720, 570, 331, 21))
        self.horizontalLine.setFrameShape(QtWidgets.QFrame.HLine)
        self.horizontalLine.setFrameShadow(QtWidgets.QFrame.Sunken)
        self.horizontalLine.setObjectName("horizontalLine")

        self.verticalLine = QtWidgets.QFrame(self.tab)
        self.verticalLine.setGeometry(QtCore.QRect(690, -20, 41, 801))
        self.verticalLine.setFrameShape(QtWidgets.QFrame.VLine)
        self.verticalLine.setFrameShadow(QtWidgets.QFrame.Sunken)
        self.verticalLine.setObjectName("verticalLine")

        self.testToBeRun = QtWidgets.QTextBrowser(self.tab)
        self.testToBeRun.setGeometry(QtCore.QRect(10, 10, 250, 41))
        self.testToBeRun.setObjectName("testToBeRun")

        self.verticalLayoutWidget = QtWidgets.QWidget(self.tab)
        self.verticalLayoutWidget.setGeometry(QtCore.QRect(720, 80, 331, 491))
        self.verticalLayoutWidget.setObjectName("verticalLayoutWidget")

        self.verticalLayout_3 = QtWidgets.QVBoxLayout(self.verticalLayoutWidget)
        self.verticalLayout_3.setContentsMargins(0, 0, 0, 0)
        self.verticalLayout_3.setObjectName("verticalLayout_3")


        self.filesList = FilesListWidget(QtWidgets.QListWidget(self.verticalLayoutWidget))
        self.filesList.setObjectName("filesList")
        self.verticalLayout_3.addWidget(self.filesList)

        self.removeSelected = QtWidgets.QPushButton(self.verticalLayoutWidget)
        self.removeSelected.setObjectName("removeSelected")
        self.removeSelected.clicked.connect(self.removeSelectedFiles)
        self.verticalLayout_3.addWidget(self.removeSelected)

        # self.dragAndDropFilesLabel = QtWidgets.QLabel(self.verticalLayoutWidget)
        # self.dragAndDropFilesLabel.setAlignment(QtCore.Qt.AlignCenter)
        # self.dragAndDropFilesLabel.setObjectName("dragAndDropFilesLabel")
        # self.verticalLayout_3.addWidget(self.dragAndDropFilesLabel)

        self.selectProjectLabel_2 = QtWidgets.QLabel(self.verticalLayoutWidget)
        self.selectProjectLabel_2.setAlignment(QtCore.Qt.AlignCenter)
        self.selectProjectLabel_2.setObjectName("selectProjectLabel_2")
        self.verticalLayout_3.addWidget(self.selectProjectLabel_2)

        self.selectProjectDropdown = QtWidgets.QComboBox(self.verticalLayoutWidget)
        self.selectProjectDropdown.setObjectName("selectProjectDropdown")
        self.populateProjects()
        self.verticalLayout_3.addWidget(self.selectProjectDropdown)



        self.radioButton = QtWidgets.QRadioButton(self.verticalLayoutWidget)
        self.radioButton.setObjectName("radioButton")
        self.verticalLayout_3.addWidget(self.radioButton)


        self.submitFiles = QtWidgets.QPushButton(self.verticalLayoutWidget)
        self.submitFiles.setObjectName("submitFiles")
        self.submitFiles.clicked.connect(self.uploadAllFiles)
        self.verticalLayout_3.addWidget(self.submitFiles)

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

        self.pushButton_2 = QtWidgets.QPushButton(self.tab_2)
        self.pushButton_2.setGeometry(QtCore.QRect(10, 420, 93, 28))
        self.pushButton_2.clicked.connect(self.changeSass)
        self.pushButton_2.setObjectName("pushButton_2")

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
        DataTiger.setWindowTitle(_translate("DataTiger", "DataTiger"))
        self.pushButton.setText(_translate("DataTiger", "generate and download"))
        self.enterHowMany.setHtml(_translate("DataTiger", "<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.0//EN\" \"http://www.w3.org/TR/REC-html40/strict.dtd\">\n"
"<html><head><meta name=\"qrichtext\" content=\"1\" /><style type=\"text/css\">\n"
"p, li { white-space: pre-wrap; }\n"
"</style></head><body style=\" font-family:\'MS Shell Dlg 2\'; font-size:7.8pt; font-weight:400; font-style:normal;\">\n"
"<p style=\"-qt-paragraph-type:empty; margin-top:0px; margin-bottom:0px; margin-left:0px; margin-right:0px; -qt-block-indent:0; text-indent:0px;\"><br /></p></body></html>"))
        self.howManyLabel.setText(_translate("DataTiger", "How many would you like?"))
        self.sortChemMaker.setHtml(_translate("DataTiger", "<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.0//EN\" \"http://www.w3.org/TR/REC-html40/strict.dtd\">\n"
"<html><head><meta name=\"qrichtext\" content=\"1\" /><style type=\"text/css\">\n"
"p, li { white-space: pre-wrap; }\n"
"</style></head><body style=\" font-family:\'MS Shell Dlg 2\'; font-size:7.8pt; font-weight:400; font-style:normal;\">\n"
"<p align=\"center\" style=\" margin-top:0px; margin-bottom:0px; margin-left:0px; margin-right:0px; -qt-block-indent:0; text-indent:0px;\"><span style=\" font-size:14pt; font-weight:600;\">Sort-Chem Maker</span></p></body></html>"))
        self.downloadTestsButton.setText(_translate("DataTiger", "Download as .csv"))
        self.FileUploadTool.setHtml(_translate("DataTiger", "<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.0//EN\" \"http://www.w3.org/TR/REC-html40/strict.dtd\">\n"
"<html><head><meta name=\"qrichtext\" content=\"1\" /><style type=\"text/css\">\n"
"p, li { white-space: pre-wrap; }\n"
"</style></head><body style=\" font-family:\'MS Shell Dlg 2\'; font-size:7.8pt; font-weight:400; font-style:normal;\">\n"
"<p align=\"center\" style=\" margin-top:0px; margin-bottom:0px; margin-left:0px; margin-right:0px; -qt-block-indent:0; text-indent:0px;\"><span style=\" font-size:14pt; font-weight:600;\">The Tiger\'s Mouth</span></p>\n"
"<p align=\"center\" style=\" margin-top:0px; margin-bottom:0px; margin-left:0px; margin-right:0px; -qt-block-indent:0; text-indent:0px;\"><span style=\" font-size:9pt;\">(Drag and Drop Files Here)</span></p></body></html>"))
        self.statusUpdateLabel.setText(_translate("DataTiger", "Status Updates"))
        self.testToBeRun.setHtml(_translate("DataTiger", "<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.0//EN\" \"http://www.w3.org/TR/REC-html40/strict.dtd\">\n"
"<html><head><meta name=\"qrichtext\" content=\"1\" /><style type=\"text/css\">\n"
"p, li { white-space: pre-wrap; }\n"
"</style></head><body style=\" font-family:\'MS Shell Dlg 2\'; font-size:7.8pt; font-weight:400; font-style:normal;\">\n"
"<p style=\" margin-top:0px; margin-bottom:0px; margin-left:0px; margin-right:0px; -qt-block-indent:0; text-indent:0px;\"><span style=\" font-size:14pt; font-weight:600;\">Missing Tiger Tests</span></p></body></html>"))
        self.selectProjectLabel_2.setText(_translate("DataTiger", "Select the default \"Project\" the files belong to."))
        # self.dragAndDropFilesLabel.setText(_translate("DataTiger", "Drag and drop the files here."))
        self.radioButton.setText(_translate("DataTiger", "Allow Duplicate Files (in the case of data-correction)"))
        self.removeSelected.setText(_translate("DataTiger", "remove selected"))
        self.submitFiles.setText(_translate("DataTiger", "submit files"))
        self.tabWidget.setTabText(self.tabWidget.indexOf(self.tab), _translate("DataTiger", "Tools"))
        self.changeSQLlabel.setText(_translate("DataTiger", "Drag and drop the database file here"))
        self.changeDBButton.setText(_translate("DataTiger", "Change Database"))
        self.changeSQLfile.setHtml(_translate("DataTiger", "<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.0//EN\" \"http://www.w3.org/TR/REC-html40/strict.dtd\">\n"
"<html><head><meta name=\"qrichtext\" content=\"1\" /><style type=\"text/css\">\n"
"p, li { white-space: pre-wrap; }\n"
"</style></head><body style=\" font-family:\'MS Shell Dlg 2\'; font-size:7.8pt; font-weight:400; font-style:normal;\">\n"
"<p style=\" margin-top:0px; margin-bottom:0px; margin-left:0px; margin-right:0px; -qt-block-indent:0; text-indent:0px;\"><span style=\" font-size:12pt; font-weight:600;\">Change SQLite database file</span></p></body></html>"))
        self.label.setText(_translate("DataTiger", "Less Sassy"))
        self.label_2.setText(_translate("DataTiger", "More Sassy"))
        self.textBrowser.setHtml(_translate("DataTiger", "<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.0//EN\" \"http://www.w3.org/TR/REC-html40/strict.dtd\">\n"
"<html><head><meta name=\"qrichtext\" content=\"1\" /><style type=\"text/css\">\n"
"p, li { white-space: pre-wrap; }\n"
"</style></head><body style=\" font-family:\'MS Shell Dlg 2\'; font-size:7.8pt; font-weight:400; font-style:normal;\">\n"
"<p style=\" margin-top:0px; margin-bottom:0px; margin-left:0px; margin-right:0px; -qt-block-indent:0; text-indent:0px;\"><span style=\" font-size:14pt; font-weight:600;\">Sass Control</span></p></body></html>"))
        self.pushButton_2.setText(_translate("DataTiger", "Apply"))
        self.tabWidget.setTabText(self.tabWidget.indexOf(self.tab_2), _translate("DataTiger", "Settings"))

#         # ******************************************************************************************
#
#
#
#
        # DataTiger.setObjectName("DataTiger")
        # DataTiger.resize(1063, 865)
        # self.mainWindow = DataTiger

#         self.centralwidget = QtWidgets.QWidget(DataTiger)
#         self.centralwidget.setObjectName("centralwidget")
#
#         self.tabWidget = QtWidgets.QTabWidget(self.centralwidget)
#         self.tabWidget.setGeometry(QtCore.QRect(0, 0, 1061, 811))
#         self.tabWidget.setObjectName("tabWidget")
#
#         self.tab = QtWidgets.QWidget()
#         self.tab.setObjectName("tab")
#
#         self.pushButton = QtWidgets.QPushButton(self.tab)
#         self.pushButton.setGeometry(QtCore.QRect(10, 720, 151, 28))
#         self.pushButton.clicked.connect(self.downloadSortChems)
#         self.pushButton.setObjectName("pushButton")
#
#         self.enterHowMany = QtWidgets.QTextEdit(self.tab)
#         self.enterHowMany.setGeometry(QtCore.QRect(10, 680, 121, 31))
#         self.enterHowMany.setObjectName("enterHowMany")
#
#         self.howManyLabel = QtWidgets.QLabel(self.tab)
#         self.howManyLabel.setGeometry(QtCore.QRect(10, 650, 161, 31))
#         self.howManyLabel.setObjectName("howManyLabel")
#
#         self.sortChemMaker = QtWidgets.QTextBrowser(self.tab)
#         self.sortChemMaker.setGeometry(QtCore.QRect(10, 610, 241, 41))
#         self.sortChemMaker.setObjectName("sortChemMaker")
#
#         self.downloadTestsButton = QtWidgets.QPushButton(self.tab)
#         self.downloadTestsButton.setGeometry(QtCore.QRect(580, 590, 121, 28))
#         self.downloadTestsButton.clicked.connect(self.downloadTestsReport)
#         self.downloadTestsButton.setObjectName("downloadTestsButton")
#
#         self.testsTable = QtWidgets.QTableWidget(self.tab)
#         self.testsTable.setGeometry(QtCore.QRect(10, 60, 691, 521))
#         self.testsTable.setObjectName("testsTable")
#
#         self.populateTable()
#
#
#         self.FileUploadTool = QtWidgets.QTextBrowser(self.tab)
#         self.FileUploadTool.setGeometry(QtCore.QRect(750, 10, 256, 61))
#         self.FileUploadTool.setObjectName("FileUploadTool")
#
#         self.statusUpdateText = QtWidgets.QTextBrowser(self.tab)
#         self.statusUpdateText.setGeometry(QtCore.QRect(720, 610, 331, 171))
#         self.statusUpdateText.append(self.easterEgg.pickRandomSaying())
#         self.statusUpdateText.setObjectName("statusUpdateText")
#
#         self.statusUpdateLabel = QtWidgets.QLabel(self.tab)
#         self.statusUpdateLabel.setGeometry(QtCore.QRect(840, 580, 91, 31))
#         self.statusUpdateLabel.setObjectName("statusUpdateLabel")
#
#         self.horizontalLine = QtWidgets.QFrame(self.tab)
#         self.horizontalLine.setGeometry(QtCore.QRect(720, 570, 331, 21))
#         self.horizontalLine.setFrameShape(QtWidgets.QFrame.HLine)
#         self.horizontalLine.setFrameShadow(QtWidgets.QFrame.Sunken)
#         self.horizontalLine.setObjectName("horizontalLine")
#
#         self.verticalLine = QtWidgets.QFrame(self.tab)
#         self.verticalLine.setGeometry(QtCore.QRect(690, -20, 41, 801))
#         self.verticalLine.setFrameShape(QtWidgets.QFrame.VLine)
#         self.verticalLine.setFrameShadow(QtWidgets.QFrame.Sunken)
#         self.verticalLine.setObjectName("verticalLine")
#
#         self.testToBeRun = QtWidgets.QTextBrowser(self.tab)
#         self.testToBeRun.setGeometry(QtCore.QRect(10, 10, 361, 41))
#         self.testToBeRun.setObjectName("testToBeRun")
#
#
#
#         self.verticalLayoutWidget = QtWidgets.QWidget(self.tab)
#         self.verticalLayoutWidget.setGeometry(QtCore.QRect(720, 80, 331, 491))
#         self.verticalLayoutWidget.setObjectName("verticalLayoutWidget")
#         #
#         self.verticalLayout_3 = QtWidgets.QVBoxLayout(self.verticalLayoutWidget)
#         self.verticalLayout_3.setContentsMargins(0, 0, 0, 0)
#         self.verticalLayout_3.setObjectName("verticalLayout_3")
#         #
#         self.selectProjectLabel = QtWidgets.QLabel(self.verticalLayoutWidget)
#         self.selectProjectLabel.setAlignment(QtCore.Qt.AlignCenter)
#         self.selectProjectLabel.setObjectName("selectProjectLabel")
#         self.verticalLayout_3.addWidget(self.selectProjectLabel)
#         #
#         self.selectProjectDropdown = QtWidgets.QComboBox(self.verticalLayoutWidget)
#         self.selectProjectDropdown.setObjectName("selectProjectDropdown")
#         self.populateProjects()
#         self.verticalLayout_3.addWidget(self.selectProjectDropdown)
#
#         self.dragAndDropFilesLabel = QtWidgets.QLabel(self.tab)
#         self.dragAndDropFilesLabel.setAlignment(QtCore.Qt.AlignCenter)
#         self.dragAndDropFilesLabel.setObjectName("dragAndDropFilesLabel")
#         self.verticalLayout_3.addWidget(self.dragAndDropFilesLabel)
#
#         self.filesList = FilesListWidget(QtWidgets.QListWidget(self.verticalLayoutWidget))
#         self.filesList.setObjectName("filesList")
#         self.verticalLayout_3.addWidget(self.filesList)
#
#         self.radioButton = QtWidgets.QRadioButton(self.verticalLayoutWidget)
#         self.radioButton.setObjectName("radioButton")
#         self.verticalLayout_3.addWidget(self.radioButton)
#
#         self.removeSelected = QtWidgets.QPushButton(self.verticalLayoutWidget)
#         self.removeSelected.setObjectName("removeSelected")
#         self.removeSelected.clicked.connect(self.removeSelectedFiles)
#         self.verticalLayout_3.addWidget(self.removeSelected)
#
#         self.submitFiles = QtWidgets.QPushButton(self.verticalLayoutWidget)
#         self.submitFiles.setObjectName("submitFiles")
#         self.submitFiles.clicked.connect(self.uploadAllFiles)
#         self.verticalLayout_3.addWidget(self.submitFiles)
#
#         self.tabWidget.addTab(self.tab, "")
#
#         self.tab_2 = QtWidgets.QWidget()
#         self.tab_2.setObjectName("tab_2")
#
#         self.enterNewDB = QtWidgets.QTextEdit(self.tab_2)
#         self.enterNewDB.setGeometry(QtCore.QRect(10, 110, 331, 131))
#         self.enterNewDB.setObjectName("enterNewDB")
#
#         self.changeSQLlabel = QtWidgets.QLabel(self.tab_2)
#         self.changeSQLlabel.setGeometry(QtCore.QRect(10, 90, 221, 16))
#         self.changeSQLlabel.setObjectName("changeSQLlabel")
#
#         self.changeDBButton = QtWidgets.QPushButton(self.tab_2)
#         self.changeDBButton.setGeometry(QtCore.QRect(10, 250, 151, 31))
#         self.changeDBButton.setObjectName("changeDBButton")
#
#         self.changeSQLfile = QtWidgets.QTextBrowser(self.tab_2)
#         self.changeSQLfile.setGeometry(QtCore.QRect(10, 40, 331, 41))
#         self.changeSQLfile.setObjectName("changeSQLfile")
#
#         self.horizontalSlider = QtWidgets.QSlider(self.tab_2)
#         self.horizontalSlider.setGeometry(QtCore.QRect(10, 360, 321, 22))
#         self.horizontalSlider.setOrientation(QtCore.Qt.Horizontal)
#         self.horizontalSlider.setObjectName("horizontalSlider")
#
#         self.label = QtWidgets.QLabel(self.tab_2)
#         self.label.setGeometry(QtCore.QRect(10, 390, 71, 16))
#         self.label.setObjectName("label")
#
#         self.label_2 = QtWidgets.QLabel(self.tab_2)
#         self.label_2.setGeometry(QtCore.QRect(260, 390, 71, 16))
#         self.label_2.setObjectName("label_2")
#
#         self.textBrowser = QtWidgets.QTextBrowser(self.tab_2)
#         self.textBrowser.setGeometry(QtCore.QRect(10, 310, 161, 41))
#         self.textBrowser.setObjectName("textBrowser")
#
#         self.pushButton_2 = QtWidgets.QPushButton(self.tab_2)
#         self.pushButton_2.setGeometry(QtCore.QRect(10, 420, 93, 28))
#         self.pushButton_2.setObjectName("pushButton_2")
#
#         self.tabWidget.addTab(self.tab_2, "")
#
#         DataTiger.setCentralWidget(self.centralwidget)
#         self.menubar = QtWidgets.QMenuBar(DataTiger)
#         self.menubar.setGeometry(QtCore.QRect(0, 0, 1063, 26))
#         self.menubar.setObjectName("menubar")
#
#         DataTiger.setMenuBar(self.menubar)
#         self.statusbar = QtWidgets.QStatusBar(DataTiger)
#         self.statusbar.setObjectName("statusbar")
#         DataTiger.setStatusBar(self.statusbar)
#
#         self.retranslateUi(DataTiger)
#         self.tabWidget.setCurrentIndex(0)
#         QtCore.QMetaObject.connectSlotsByName(DataTiger)
#
#     def retranslateUi(self, DataTiger):
#         _translate = QtCore.QCoreApplication.translate
#         DataTiger.setWindowTitle(_translate("DataTiger", "MainWindow"))
#         self.pushButton.setText(_translate("DataTiger", "generate and download"))
#         self.enterHowMany.setHtml(_translate("DataTiger", "<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.0//EN\" \"http://www.w3.org/TR/REC-html40/strict.dtd\">\n"
# "<html><head><meta name=\"qrichtext\" content=\"1\" /><style type=\"text/css\">\n"
# "p, li { white-space: pre-wrap; }\n"
# "</style></head><body style=\" font-family:\'MS Shell Dlg 2\'; font-size:7.8pt; font-weight:400; font-style:normal;\">\n"
# "<p style=\"-qt-paragraph-type:empty; margin-top:0px; margin-bottom:0px; margin-left:0px; margin-right:0px; -qt-block-indent:0; text-indent:0px;\"><br /></p></body></html>"))
#         self.howManyLabel.setText(_translate("DataTiger", "How many would you like?"))
#         self.sortChemMaker.setHtml(_translate("DataTiger", "<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.0//EN\" \"http://www.w3.org/TR/REC-html40/strict.dtd\">\n"
# "<html><head><meta name=\"qrichtext\" content=\"1\" /><style type=\"text/css\">\n"
# "p, li { white-space: pre-wrap; }\n"
# "</style></head><body style=\" font-family:\'MS Shell Dlg 2\'; font-size:7.8pt; font-weight:400; font-style:normal;\">\n"
# "<p align=\"center\" style=\" margin-top:0px; margin-bottom:0px; margin-left:0px; margin-right:0px; -qt-block-indent:0; text-indent:0px;\"><span style=\" font-size:14pt; font-weight:600;\">Sort-Chem Maker</span></p></body></html>"))
#         self.downloadTestsButton.setText(_translate("DataTiger", "Download as .csv"))
#         # self.FileUploadTool.setHtml(_translate("DataTiger", "<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.0//EN\" \"http://www.w3.org/TR/REC-html40/strict.dtd\">\n"
# # "<html><head><meta name=\"qrichtext\" content=\"1\" /><style type=\"text/css\">\n"
# # "p, li { white-space: pre-wrap; }\n"
# # "</style></head><body style=\" font-family:\'MS Shell Dlg 2\'; font-size:7.8pt; font-weight:400; font-style:normal;\">\n"
# # "<p align=\"center\" style=\" margin-top:0px; margin-bottom:0px; margin-left:0px; margin-right:0px; -qt-block-indent:0; text-indent:0px;\"><span style=\" font-size:14pt; font-weight:600;\">The Tiger\'s Mouth</span></p>\n"
# # "<p align=\"center\" style=\" margin-top:0px; margin-bottom:0px; margin-left:0px; margin-right:0px; -qt-block-indent:0; text-indent:0px;\"><span style=\" font-size:9pt;\">(File Upload Tool)</span></p></body></html>"))
# #         self.statusUpdateLabel.setText(_translate("DataTiger", "Status Updates"))
# #         self.testToBeRun.setHtml(_translate("DataTiger", "<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.0//EN\" \"http://www.w3.org/TR/REC-html40/strict.dtd\">\n"
# # "<html><head><meta name=\"qrichtext\" content=\"1\" /><style type=\"text/css\">\n"
# # "p, li { white-space: pre-wrap; }\n"
# # "</style></head><body style=\" font-family:\'MS Shell Dlg 2\'; font-size:7.8pt; font-weight:400; font-style:normal;\">\n"
# # "<p style=\" margin-top:0px; margin-bottom:0px; margin-left:0px; margin-right:0px; -qt-block-indent:0; text-indent:0px;\"><span style=\" font-size:14pt; font-weight:600;\">Tests that have yet to be run</span></p></body></html>"))
#         self.selectProjectLabel.setText(_translate("DataTiger", "Select the default \"Project\" the files belong to."))
#         self.dragAndDropFilesLabel.setText(_translate("DataTiger", "Drag and drop the files here."))
# #         self.radioButton.setText(_translate("DataTiger", "Allow Duplicate Files (in the case of data-correction)"))
# #         self.removeSelected.setText(_translate("DataTiger", "remove selected"))
# #         self.submitFiles.setText(_translate("DataTiger", "submit files"))
#         self.tabWidget.setTabText(self.tabWidget.indexOf(self.tab), _translate("DataTiger", "Tools"))
#         self.changeSQLlabel.setText(_translate("DataTiger", "Drag and drop the database file here"))
#         self.changeDBButton.setText(_translate("DataTiger", "Change Database"))
#         self.changeSQLfile.setHtml(_translate("DataTiger", "<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.0//EN\" \"http://www.w3.org/TR/REC-html40/strict.dtd\">\n"
# "<html><head><meta name=\"qrichtext\" content=\"1\" /><style type=\"text/css\">\n"
# "p, li { white-space: pre-wrap; }\n"
# "</style></head><body style=\" font-family:\'MS Shell Dlg 2\'; font-size:7.8pt; font-weight:400; font-style:normal;\">\n"
# "<p style=\" margin-top:0px; margin-bottom:0px; margin-left:0px; margin-right:0px; -qt-block-indent:0; text-indent:0px;\"><span style=\" font-size:12pt; font-weight:600;\">Change SQLite database file</span></p></body></html>"))
#         self.label.setText(_translate("DataTiger", "Less Sassy"))
#         self.label_2.setText(_translate("DataTiger", "More Sassy"))
#         self.textBrowser.setHtml(_translate("DataTiger", "<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.0//EN\" \"http://www.w3.org/TR/REC-html40/strict.dtd\">\n"
# "<html><head><meta name=\"qrichtext\" content=\"1\" /><style type=\"text/css\">\n"
# "p, li { white-space: pre-wrap; }\n"
# "</style></head><body style=\" font-family:\'MS Shell Dlg 2\'; font-size:7.8pt; font-weight:400; font-style:normal;\">\n"
# "<p style=\" margin-top:0px; margin-bottom:0px; margin-left:0px; margin-right:0px; -qt-block-indent:0; text-indent:0px;\"><span style=\" font-size:14pt; font-weight:600;\">Sass Control</span></p></body></html>"))
#         self.pushButton_2.setText(_translate("DataTiger", "Apply"))
#         self.tabWidget.setTabText(self.tabWidget.indexOf(self.tab_2), _translate("DataTiger", "Settings"))


if __name__ == "__main__":
    import sys
    app = QtWidgets.QApplication(sys.argv)
    DataTiger = QtWidgets.QMainWindow()
    ui = Ui_DataTiger()
    db = Database()
    ui.setupUi(DataTiger, db)
    DataTiger.show()
    sys.exit(app.exec_())

